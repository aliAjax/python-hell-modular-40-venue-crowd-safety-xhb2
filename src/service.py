from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, ValidationError
from .repository import utcnow
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def upgrade(self):
        """Backfill team occupancy for legacy data; safe to run repeatedly."""
        self.repository.backfill_teams()
        teams = self.repository.list_entities(kind="team")
        return {
            "teams": len(teams),
            "occupied": sum(1 for team in teams if team["status"] == "occupied"),
        }

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        kind = self.rules.normalize_kind(entity["kind"])
        if kind == "task" and action in ("assign", "complete", "cancel"):
            updated = self.repository.set_task_team(
                task_id=entity_id,
                expected_version=expected,
                next_status=next_status,
                patch=patch,
                team_id=entity["data"].get("team_id"),
                occupy=(action == "assign"),
            )
        else:
            merged = dict(entity["data"])
            merged.update(patch)
            updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def reassign(self, actor, task_id, data, expected_version=None):
        entity = self.repository.get_entity(task_id)
        if not entity:
            raise NotFoundError("entity not found: " + task_id)
        if self.rules.normalize_kind(entity["kind"]) != "task":
            raise ValidationError("only tasks can be reassigned")
        info = self.rules.validate_reassign(actor, entity, dict(data or {}), self._lookup)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        updated = self.repository.reassign_task(
            task_id=task_id,
            expected_task_version=expected,
            new_team_id=info["new_team_id"],
            expected_new_team_version=data.get("expected_team_version"),
            actor_id=actor.user_id,
            actor_role=actor.role,
            reason=info["reason"],
            incident_priority=info["incident_priority"],
            incident_status=info["incident"]["status"],
            reassigned_at=data.get("reassigned_at") or utcnow(),
        )
        self.audit.record(
            task_id,
            actor,
            "reassign",
            entity["status"],
            updated["status"],
            {
                "from_team_id": entity["data"].get("team_id"),
                "to_team_id": info["new_team_id"],
                "reason": info["reason"],
                "incident_id": entity["data"].get("incident_id"),
                "incident_priority": info["incident_priority"],
            },
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
