import json
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

from .domain import ConflictError, NotFoundError, ValidationError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


ACTIVE_TASK_STATUSES = ("assigned", "enroute", "on_scene")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
            """)
        self.backfill_teams()

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    # ---- team occupancy helpers (run inside a single write transaction) ----

    def _get_row(self, connection, entity_id):
        return connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()

    def _find_team_row(self, connection, team_business_id):
        rows = connection.execute(
            "SELECT * FROM entities WHERE kind = 'team'"
        ).fetchall()
        for row in rows:
            if json.loads(row["data"]).get("team_id") == team_business_id:
                return row
        return None

    def _insert_team_tx(self, connection, team_business_id, current_task_id):
        now = utcnow()
        entity_id = str(uuid4())
        data = {"team_id": team_business_id, "current_task_id": current_task_id}
        status = "occupied" if current_task_id else "available"
        connection.execute(
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, 'team', ?, 1, ?, 'system', ?, ?)",
            (entity_id, status, json.dumps(data, ensure_ascii=False, sort_keys=True), now, now),
        )
        return self._entity_from_row(self._get_row(connection, entity_id))

    def _ensure_team_tx(self, connection, team_business_id):
        row = self._find_team_row(connection, team_business_id)
        if row:
            return self._entity_from_row(row)
        return self._insert_team_tx(connection, team_business_id, None)

    def _update_entity_tx(self, connection, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        cursor = connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (status, payload, now, entity_id, int(expected_version)),
        )
        if cursor.rowcount == 0:
            raise ConflictError(
                "version conflict or missing entity: %s (expected %s)"
                % (entity_id, expected_version)
            )
        return self._entity_from_row(self._get_row(connection, entity_id))

    def reassign_task(self, task_id, expected_task_version, new_team_id, expected_new_team_version,
                      actor_id, actor_role, reason, incident_priority, incident_status, reassigned_at):
        """Atomically move a task to a new team: release the old team, occupy the new one.

        The whole move runs in one BEGIN IMMEDIATE transaction, so a failed write
        rolls back completely and a retry never leaves both teams occupied.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            task_row = self._get_row(connection, task_id)
            if not task_row:
                raise NotFoundError("entity not found: " + task_id)
            task = self._entity_from_row(task_row)
            if task["kind"] != "task":
                raise ValidationError("only tasks can be reassigned")
            old_team_id = task["data"].get("team_id")
            if old_team_id == new_team_id:
                raise ValidationError("new team must differ from the current team")
            if expected_task_version is not None and task["version"] != int(expected_task_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_task_version, task["version"])
                )
            old_team_row = self._find_team_row(connection, old_team_id) if old_team_id else None
            old_team = self._entity_from_row(old_team_row) if old_team_row else None
            new_team = self._ensure_team_tx(connection, new_team_id)
            if new_team["data"].get("current_task_id"):
                raise ConflictError(
                    "team %s is already occupied by task %s"
                    % (new_team_id, new_team["data"]["current_task_id"])
                )
            if expected_new_team_version is not None and new_team["version"] != int(expected_new_team_version):
                raise ConflictError(
                    "team version conflict: expected %s, found %s"
                    % (expected_new_team_version, new_team["version"])
                )
            if old_team and old_team["data"].get("current_task_id") not in (None, task_id):
                raise ConflictError(
                    "original team %s does not currently hold task %s" % (old_team_id, task_id)
                )
            new_task_data = dict(task["data"])
            history = list(new_task_data.get("reassign_history") or [])
            history.append({
                "from_team_id": old_team_id,
                "to_team_id": new_team_id,
                "actor_id": actor_id,
                "actor_role": actor_role,
                "reason": reason,
                "incident_priority": incident_priority,
                "incident_status_at_reassign": incident_status,
                "reassigned_at": reassigned_at,
            })
            new_task_data["team_id"] = new_team_id
            new_task_data["reassign_history"] = history
            new_task_data["last_reassigned_at"] = reassigned_at
            updated_task = self._update_entity_tx(
                connection, task_id, task["version"], task["status"], new_task_data
            )
            if old_team:
                old_data = dict(old_team["data"])
                old_data["current_task_id"] = None
                self._update_entity_tx(
                    connection, old_team["id"], old_team["version"], "available", old_data
                )
            new_data = dict(new_team["data"])
            new_data["current_task_id"] = task_id
            self._update_entity_tx(
                connection, new_team["id"], new_team["version"], "occupied", new_data
            )
            connection.commit()
            return updated_task
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def set_task_team(self, task_id, expected_version, next_status, patch, team_id, occupy):
        """Update a task's lifecycle status together with its team's occupancy in one transaction."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            task_row = self._get_row(connection, task_id)
            if not task_row:
                raise NotFoundError("entity not found: " + task_id)
            task = self._entity_from_row(task_row)
            if task["kind"] != "task":
                raise ValidationError("only tasks have teams")
            if expected_version is not None and task["version"] != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, task["version"])
                )
            merged = dict(task["data"])
            merged.update(patch)
            updated_task = self._update_entity_tx(
                connection, task_id, task["version"], next_status, merged
            )
            team = self._ensure_team_tx(connection, team_id)
            team_data = dict(team["data"])
            if occupy:
                if team_data.get("current_task_id") and team_data["current_task_id"] != task_id:
                    raise ConflictError(
                        "team %s is already occupied by task %s"
                        % (team_id, team_data["current_task_id"])
                    )
                team_data["current_task_id"] = task_id
                team_status = "occupied"
            else:
                team_data["current_task_id"] = None
                team_status = "available"
            self._update_entity_tx(
                connection, team["id"], team["version"], team_status, team_data
            )
            connection.commit()
            return updated_task
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def backfill_teams(self):
        """Upgrade legacy data: ensure every team_id has a team row and fill its current task.

        Idempotent. Teams with an active (assigned/enroute/on_scene) task are marked
        occupied with that task; all others are available with no current task.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            task_rows = connection.execute(
                "SELECT * FROM entities WHERE kind = 'task'"
            ).fetchall()
            active_by_team = {}
            all_team_ids = set()
            for row in task_rows:
                task = self._entity_from_row(row)
                tid = task["data"].get("team_id")
                if not tid:
                    continue
                all_team_ids.add(tid)
                if task["status"] in ACTIVE_TASK_STATUSES:
                    previous = active_by_team.get(tid)
                    if previous is None or task["updated_at"] > previous["updated_at"]:
                        active_by_team[tid] = task
            team_rows = connection.execute(
                "SELECT * FROM entities WHERE kind = 'team'"
            ).fetchall()
            existing = {
                self._entity_from_row(row)["data"].get("team_id"): self._entity_from_row(row)
                for row in team_rows
            }
            for tid in all_team_ids:
                target_task = active_by_team.get(tid)
                target_id = target_task["id"] if target_task else None
                team = existing.get(tid)
                if team is None:
                    self._insert_team_tx(connection, tid, target_id)
                    continue
                current = team["data"].get("current_task_id")
                if current != target_id:
                    data = dict(team["data"])
                    data["current_task_id"] = target_id
                    status = "occupied" if target_id else "available"
                    self._update_entity_tx(
                        connection, team["id"], team["version"], status, data
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
