import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from src.domain import (
    Actor,
    ConflictError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
)
from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def _reassign_data(to_team, incident_version, reason="medical point overwhelmed", at="2026-09-27T18:15:00Z"):
    return {
        "new_team_id": to_team,
        "incident_version": incident_version,
        "reason": reason,
        "reassigned_at": at,
    }


class ReassignmentTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repository = SQLiteRepository(Path(self.tmp.name) / "reassign.db")
        self.service = DomainService(self.repository, RuleEngine())
        self.coordinator = Actor("commander-1", "coordinator")
        self.coordinator2 = Actor("commander-2", "coordinator")
        self.supervisor = Actor("supervisor", "supervisor")
        self.operator = Actor("operator", "operator")
        self.viewer = Actor("viewer", "viewer")

    def tearDown(self):
        self.tmp.cleanup()

    def _dispatch_enroute_task(self, team_id, source_ref="radio-1", severity="high"):
        venue = self.service.create(
            self.coordinator, "venue", {"name": "Grand Hall", "address": "1 Stadium Road"}
        )
        zone = self.service.create(
            self.coordinator,
            "zone",
            {"venue_id": venue["id"], "name": "North Stand", "capacity": 1000},
        )
        incident = self.service.create(
            self.operator,
            "incident",
            {
                "venue_id": venue["id"],
                "zone_id": zone["id"],
                "source_ref": source_ref,
                "incident_type": "medical",
                "severity": severity,
                "reported_at": "2026-09-27T18:00:00Z",
            },
        )
        incident = self.service.transition(
            self.supervisor, incident["id"], "triage", {"priority": "medical"}
        )
        incident = self.service.transition(
            self.coordinator, incident["id"], "dispatch", {"commander_id": "commander-1"}
        )
        task = self.service.create(
            self.supervisor,
            "task",
            {
                "incident_id": incident["id"],
                "venue_id": venue["id"],
                "zone_id": zone["id"],
                "team_id": team_id,
                "task_type": "medical",
            },
        )
        task = self.service.transition(
            self.coordinator, task["id"], "assign", {"assigned_at": "2026-09-27T18:11:00Z"}
        )
        task = self.service.transition(
            self.operator, task["id"], "acknowledge", {"acknowledged_at": "2026-09-27T18:12:00Z"}
        )
        return venue, zone, incident, task

    def test_reassign_swaps_team_keeps_enroute_and_records_rework(self):
        _, _, incident, task = self._dispatch_enroute_task("team-a")
        self.assertEqual(task["status"], "enroute")
        states = {row["team_id"]: row["current_task_id"] for row in self.service.team_states()}
        self.assertEqual(states["team-a"], task["id"])
        self.assertNotIn("team-b", states)

        moved = self.service.transition(
            self.coordinator,
            task["id"],
            "reassign",
            _reassign_data("team-b", incident["version"]),
        )
        # 任务仍在途，只是换了承接班组。
        self.assertEqual(moved["status"], "enroute")
        self.assertEqual(moved["data"]["team_id"], "team-b")
        self.assertEqual(moved["data"]["reassigned_by"], "commander-1")
        # 版本递增，乐观锁可感知。
        self.assertEqual(moved["version"], task["version"] + 1)

        # 原班组同时释放，新班组占用任务。
        states = {row["team_id"]: row["current_task_id"] for row in self.service.team_states()}
        self.assertNotIn("team-a", states)
        self.assertEqual(states["team-b"], task["id"])

        # 被换下的任务保留返工记录，含事件优先级。
        history = moved["data"]["reassignment_history"]
        self.assertEqual(len(history), 1)
        entry = history[0]
        self.assertEqual(entry["from_team_id"], "team-a")
        self.assertEqual(entry["to_team_id"], "team-b")
        self.assertEqual(entry["reason"], "medical point overwhelmed")
        self.assertEqual(entry["incident_id"], incident["id"])
        self.assertEqual(entry["incident_priority"], incident["data"]["priority_score"])

        audit = self.service.audit_log(task["id"])
        actions = [row["action"] for row in audit]
        self.assertIn("reassign", actions)

    def test_two_commanders_same_target_team_only_one_wins(self):
        _, _, inc1, task1 = self._dispatch_enroute_task("team-a1", source_ref="radio-1")
        _, _, inc2, task2 = self._dispatch_enroute_task("team-a2", source_ref="radio-2")
        barrier = threading.Barrier(2)
        results = []

        def commander(task, incident, actor):
            barrier.wait()
            try:
                self.service.transition(
                    actor,
                    task["id"],
                    "reassign",
                    _reassign_data("team-b", incident["version"]),
                )
                results.append(("ok", task["id"]))
            except ConflictError as exc:
                results.append(("conflict", str(exc)))

        t1 = threading.Thread(target=commander, args=(task1, inc1, self.coordinator))
        t2 = threading.Thread(target=commander, args=(task2, inc2, self.coordinator2))
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        self.assertEqual(len(results), 2)
        self.assertEqual(sorted(status for status, _ in results), ["conflict", "ok"])
        # 后到者看到占用冲突，唯一的胜者拿到新班组。
        states = {row["team_id"]: row["current_task_id"] for row in self.service.team_states()}
        self.assertEqual(states["team-b"], results[0][1] if results[0][0] == "ok" else results[1][1])
        loser_task = task2["id"] if results[0][0] == "ok" and results[0][1] == task1["id"] else task1["id"]
        self.assertEqual(self.service.get(loser_task)["data"]["team_id"], "team-a1" if loser_task == task1["id"] else "team-a2")

    def test_failed_write_rolls_back_and_retried_request_succeeds(self):
        _, _, incident, task = self._dispatch_enroute_task("team-a")
        original = self.repository._set_team_state
        calls = {"count": 0}

        def flaky(connection, team_id, task_id, now):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("simulated storage outage")
            return original(connection, team_id, task_id, now)

        self.repository._set_team_state = flaky
        with self.assertRaises(RuntimeError):
            self.service.transition(
                self.coordinator,
                task["id"],
                "reassign",
                _reassign_data("team-b", incident["version"]),
            )
        self.repository._set_team_state = original

        # 失败后没有把两个班组都占住：原队仍持有任务，新队从未落库。
        states = {row["team_id"]: row["current_task_id"] for row in self.service.team_states()}
        self.assertEqual(states.get("team-a"), task["id"])
        self.assertNotIn("team-b", states)
        unchanged = self.service.get(task["id"])
        self.assertEqual(unchanged["data"]["team_id"], "team-a")
        self.assertEqual(unchanged["version"], task["version"])

        # 同一改派请求可以安全重试。
        retried = self.service.transition(
            self.coordinator,
            task["id"],
            "reassign",
            _reassign_data("team-b", incident["version"]),
        )
        self.assertEqual(retried["data"]["team_id"], "team-b")
        states = {row["team_id"]: row["current_task_id"] for row in self.service.team_states()}
        self.assertNotIn("team-a", states)
        self.assertEqual(states["team-b"], task["id"])

    def test_stale_reassignment_fails_after_incident_changes(self):
        _, _, incident, task = self._dispatch_enroute_task("team-a")
        prepared_version = incident["version"]

        # 事件先被处置结束：旧改派失效，且事件不再活跃。
        incident = self.service.transition(
            self.coordinator, incident["id"], "resolve", {"resolution": "handled on scene"}
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator,
                task["id"],
                "reassign",
                _reassign_data("team-b", prepared_version),
            )

        # 事件重开后版本已变，拿着旧版本仍要被拒绝。
        incident = self.service.transition(
            self.coordinator, incident["id"], "reopen", {"reason": "patient relapsed"}
        )
        self.assertNotEqual(incident["version"], prepared_version)
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator,
                task["id"],
                "reassign",
                _reassign_data("team-b", prepared_version),
            )
        # 用新版本重新准备后改派成功。
        moved = self.service.transition(
            self.coordinator,
            task["id"],
            "reassign",
            _reassign_data("team-b", incident["version"]),
        )
        self.assertEqual(moved["data"]["team_id"], "team-b")

    def test_unauthorized_role_is_rejected(self):
        _, _, incident, task = self._dispatch_enroute_task("team-a")
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                self.viewer, task["id"], "reassign", _reassign_data("team-b", incident["version"])
            )
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                self.supervisor, task["id"], "reassign", _reassign_data("team-b", incident["version"])
            )

    def test_only_enroute_tasks_can_be_reassigned(self):
        _, _, incident, task = self._dispatch_enroute_task("team-a")
        arrived = self.service.transition(
            self.operator, task["id"], "arrive", {"arrived_at": "2026-09-27T18:14:00Z"}
        )
        self.assertEqual(arrived["status"], "on_scene")
        with self.assertRaises(InvalidTransition):
            self.service.transition(
                self.coordinator,
                task["id"],
                "reassign",
                _reassign_data("team-b", incident["version"]),
            )

    def test_target_team_busy_and_same_team_are_rejected(self):
        _, _, incident, task = self._dispatch_enroute_task("team-a")
        # team-b 自己已有一个在途任务。
        _, _, _, busy_task = self._dispatch_enroute_task("team-b", source_ref="radio-9")
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator,
                task["id"],
                "reassign",
                _reassign_data("team-b", incident["version"]),
            )
        # busy_task 仍在 team-b，task 仍在 team-a。
        self.assertEqual(self.service.get(busy_task["id"])["data"]["team_id"], "team-b")
        self.assertEqual(self.service.get(task["id"])["data"]["team_id"], "team-a")

        with self.assertRaises(ValidationError):
            self.service.transition(
                self.coordinator,
                task["id"],
                "reassign",
                _reassign_data("team-a", incident["version"]),
            )
        with self.assertRaises(ValidationError):
            self.service.transition(
                self.coordinator,
                task["id"],
                "reassign",
                {
                    "new_team_id": "team-c",
                    "incident_version": incident["version"],
                    "reassigned_at": "2026-09-27T18:15:00Z",
                },
            )

    def test_completion_releases_team_state(self):
        _, _, _, task = self._dispatch_enroute_task("team-a")
        task = self.service.transition(
            self.operator, task["id"], "arrive", {"arrived_at": "2026-09-27T18:14:00Z"}
        )
        self.service.transition(
            self.operator,
            task["id"],
            "complete",
            {"completed_at": "2026-09-27T18:20:00Z", "outcome": "transferred"},
        )
        states = {row["team_id"]: row["current_task_id"] for row in self.service.team_states()}
        self.assertNotIn("team-a", states)


class ReassignmentMigrationTest(unittest.TestCase):
    def test_legacy_database_backfills_current_team_tasks(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "legacy.db"
        # 构造一个没有 team_state 表的旧版本库。
        with sqlite3.connect(db_path) as raw:
            raw.executescript("""
                CREATE TABLE entities (
                    id TEXT PRIMARY KEY, kind TEXT, status TEXT, version INTEGER,
                    data TEXT, created_by TEXT, created_at TEXT, updated_at TEXT
                );
                CREATE TABLE audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT, actor_id TEXT,
                    actor_role TEXT, action TEXT, from_status TEXT, to_status TEXT,
                    detail TEXT, created_at TEXT
                );
                CREATE TABLE idempotency (
                    actor_id TEXT, idem_key TEXT, entity_id TEXT, created_at TEXT,
                    PRIMARY KEY(actor_id, idem_key)
                );
            """)
            rows = [
                ("t-enroute", "enroute", "team-x"),
                ("t-assigned", "assigned", "team-y"),
                ("t-done", "completed", "team-x"),
                ("t-draft", "draft", "team-z"),
            ]
            for entity_id, status, team_id in rows:
                raw.execute(
                    "INSERT INTO entities VALUES (?, 'task', ?, 1, ?, 'u', 't', 't')",
                    (entity_id, status, json.dumps({"team_id": team_id})),
                )

        repository = SQLiteRepository(db_path)
        states = {row["team_id"]: row["current_task_id"] for row in repository.list_team_states()}
        # 每队当前任务从活跃任务补齐；已完成/草稿不占位，同队取最新活跃任务。
        self.assertEqual(states.get("team-x"), "t-enroute")
        self.assertEqual(states.get("team-y"), "t-assigned")
        self.assertNotIn("team-z", states)
        with sqlite3.connect(db_path) as raw:
            self.assertEqual(int(raw.execute("PRAGMA user_version").fetchone()[0]), 1)

        # 升级后改派直接可用。
        service = DomainService(repository, RuleEngine())
        venue = service.create(Actor("c", "coordinator"), "venue", {"name": "V", "address": "A"})
        zone = service.create(
            Actor("c", "coordinator"),
            "zone",
            {"venue_id": venue["id"], "name": "Z", "capacity": 100},
        )
        incident = service.create(
            Actor("o", "operator"),
            "incident",
            {
                "venue_id": venue["id"],
                "zone_id": zone["id"],
                "source_ref": "r1",
                "incident_type": "medical",
                "severity": "high",
                "reported_at": "2026-09-27T18:00:00Z",
            },
        )
        incident = service.transition(Actor("s", "supervisor"), incident["id"], "triage", {"priority": "medical"})
        incident = service.transition(Actor("c", "coordinator"), incident["id"], "dispatch", {"commander_id": "c"})
        legacy_task = repository.get_entity("t-enroute")
        legacy_task["data"]["incident_id"] = incident["id"]
        repository.update_entity("t-enroute", 1, "enroute", legacy_task["data"])
        moved = service.transition(
            Actor("c", "coordinator"),
            "t-enroute",
            "reassign",
            _reassign_data("team-q", incident["version"]),
        )
        self.assertEqual(moved["data"]["team_id"], "team-q")


class ReassignmentHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "http.db"), RuleEngine()
        )
        self.server = create_server("127.0.0.1", 0, self.service, RuleEngine(), self.tmp.name)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def _post(self, path, body, role, user="u"):
        request = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path),
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-User-Id": user, "X-Role": role},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def _enroute(self, team_id):
        status, venue = self._post("/api/venues", {"name": "V", "address": "A"}, "coordinator")
        status, zone = self._post(
            "/api/zones", {"venue_id": venue["id"], "name": "Z", "capacity": 100}, "coordinator"
        )
        status, incident = self._post(
            "/api/incidents",
            {
                "venue_id": venue["id"],
                "zone_id": zone["id"],
                "source_ref": "r1",
                "incident_type": "medical",
                "severity": "high",
                "reported_at": "2026-09-27T18:00:00Z",
            },
            "operator",
        )
        self._post(
            "/api/entities/%s/actions" % incident["id"],
            {"action": "triage", "data": {"priority": "medical"}},
            "supervisor",
        )
        status, incident = self._post(
            "/api/entities/%s/actions" % incident["id"],
            {"action": "dispatch", "data": {"commander_id": "c"}},
            "coordinator",
        )
        status, task = self._post(
            "/api/tasks",
            {
                "incident_id": incident["id"],
                "venue_id": venue["id"],
                "zone_id": zone["id"],
                "team_id": team_id,
                "task_type": "medical",
            },
            "supervisor",
        )
        self._post(
            "/api/entities/%s/actions" % task["id"],
            {"action": "assign", "data": {"assigned_at": "t1"}},
            "coordinator",
        )
        status, task = self._post(
            "/api/entities/%s/actions" % task["id"],
            {"action": "acknowledge", "data": {"acknowledged_at": "t2"}},
            "operator",
        )
        return incident, task

    def test_http_permission_denied_and_successful_reassign(self):
        incident, task = self._enroute("team-a")
        status, payload = self._post(
            "/api/entities/%s/actions" % task["id"],
            {"action": "reassign", "data": _reassign_data("team-b", incident["version"])},
            "viewer",
        )
        self.assertEqual(status, 403)
        self.assertEqual(payload["type"], "PermissionDenied")

        status, moved = self._post(
            "/api/entities/%s/actions" % task["id"],
            {"action": "reassign", "data": _reassign_data("team-b", incident["version"])},
            "coordinator",
        )
        self.assertEqual(status, 200)
        self.assertEqual(moved["data"]["team_id"], "team-b")
        self.assertEqual(moved["status"], "enroute")


if __name__ == "__main__":
    unittest.main()
