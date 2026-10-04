import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, InvalidTransition, NotFoundError, ValidationError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


ACTIVE_TASK_STATUSES = ("assigned", "enroute", "on_scene")
SCHEMA_VERSION = 1


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
                CREATE TABLE IF NOT EXISTS team_state (
                    team_id TEXT PRIMARY KEY,
                    current_task_id TEXT,
                    updated_at TEXT NOT NULL
                );
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
            self._migrate(connection)

    def _migrate(self, connection):
        """把旧版本数据补齐到当前结构。

        版本0（旧库）没有 team_state：每队当前任务需从活跃任务反推补齐。
        """
        current = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if current < 1:
            now = utcnow()
            placeholders = ",".join("?" for _ in ACTIVE_TASK_STATUSES)
            rows = connection.execute(
                "SELECT id, status, data FROM entities WHERE kind = 'task' "
                "AND status IN (%s) ORDER BY created_at, id" % placeholders,
                ACTIVE_TASK_STATUSES,
            ).fetchall()
            for row in rows:
                team_id = (json.loads(row["data"]) or {}).get("team_id")
                if not team_id:
                    continue
                # 同一队理论上只有一个活跃任务；若旧数据已有多个，保留最新一个。
                connection.execute(
                    "INSERT OR REPLACE INTO team_state(team_id, current_task_id, updated_at) "
                    "VALUES (?, ?, ?)",
                    (team_id, row["id"], now),
                )
            connection.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
        connection.commit()

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

    @staticmethod
    def _set_team_state(connection, team_id, task_id, now):
        """把队的当前任务写入 team_state；队为空时删除占位行。"""
        if task_id is None:
            connection.execute("DELETE FROM team_state WHERE team_id = ?", (team_id,))
            return
        row = connection.execute(
            "SELECT 1 FROM team_state WHERE team_id = ?", (team_id,)
        ).fetchone()
        if row:
            connection.execute(
                "UPDATE team_state SET current_task_id = ?, updated_at = ? WHERE team_id = ?",
                (task_id, now, team_id),
            )
        else:
            connection.execute(
                "INSERT INTO team_state(team_id, current_task_id, updated_at) VALUES (?, ?, ?)",
                (team_id, task_id, now),
            )

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT kind, version FROM entities WHERE id = ?", (entity_id,)
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
            if row["kind"] == "task":
                team_id = (data or {}).get("team_id")
                if team_id:
                    if status in ACTIVE_TASK_STATUSES:
                        # 防御性检查：活跃任务占用队伍时，队伍不得已被其他任务占用。
                        occupied = connection.execute(
                            "SELECT current_task_id FROM team_state WHERE team_id = ?", (team_id,)
                        ).fetchone()
                        if occupied and occupied["current_task_id"] != entity_id:
                            raise ConflictError(
                                "team %s already has active task %s"
                                % (team_id, occupied["current_task_id"])
                            )
                        self._set_team_state(connection, team_id, entity_id, now)
                    else:
                        self._set_team_state(connection, team_id, None, now)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def reassign_task(
        self,
        task_id,
        expected_task_version,
        expected_incident_version,
        new_team_id,
        data,
        from_status,
        to_status,
    ):
        """在单个事务内完成改派：校验占用、换班组、释放原队、占用新队。

        BEGIN IMMEDIATE 让两个指挥员对同一班组的并发改派串行化：
        后到者在锁内看到占用冲突，整个事务回滚，调用方可安全重试。
        """
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            task_row = connection.execute(
                "SELECT version, status, data FROM entities WHERE id = ?", (task_id,)
            ).fetchone()
            if not task_row:
                raise NotFoundError("entity not found: " + task_id)
            task_version = int(task_row["version"])
            if expected_task_version is not None and task_version != int(expected_task_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_task_version, task_version)
                )
            if task_row["status"] != from_status:
                raise InvalidTransition(
                    "cannot reassign from status %s" % task_row["status"]
                )
            task_data = json.loads(task_row["data"])
            incident_id = task_data.get("incident_id")
            current_team_id = task_data.get("team_id")
            incident_row = connection.execute(
                "SELECT version, status FROM entities WHERE id = ?", (incident_id,)
            ).fetchone()
            if not incident_row:
                raise ConflictError("task incident no longer exists")
            if incident_row["status"] not in ("dispatched", "reopened"):
                raise ConflictError("incident is not active for reassignment")
            # 事件状态变化后，基于旧版本准备的改派立即失效。
            if int(incident_row["version"]) != int(expected_incident_version):
                raise ConflictError(
                    "incident version mismatch: prepared %s, current %s"
                    % (expected_incident_version, incident_row["version"])
                )
            if not new_team_id or new_team_id == current_team_id:
                raise ValidationError("new team must differ from the current team")
            occupied = connection.execute(
                "SELECT current_task_id FROM team_state WHERE team_id = ?", (new_team_id,)
            ).fetchone()
            if occupied and occupied["current_task_id"] != task_id:
                raise ConflictError(
                    "target team %s already has active task %s"
                    % (new_team_id, occupied["current_task_id"])
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (to_status, payload, now, task_id, task_version),
            )
            self._set_team_state(connection, current_team_id, None, now)
            self._set_team_state(connection, new_team_id, task_id, now)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(task_id)

    def list_team_states(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM team_state ORDER BY team_id"
            ).fetchall()
        return [
            {"team_id": row["team_id"], "current_task_id": row["current_task_id"], "updated_at": row["updated_at"]}
            for row in rows
        ]

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

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
