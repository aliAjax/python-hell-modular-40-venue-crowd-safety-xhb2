from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError
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
        payload = dict(data or {})
        if self.rules.normalize_kind(entity["kind"]) == "task" and action == "reassign":
            return self._reassign(actor, entity, payload, expected_version)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, payload, self._lookup
        )
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

    def _reassign(self, actor, entity, payload, expected_version):
        # 先在规则层做角色、必填字段和状态的快速校验（含读快照的占用检查），
        # 真正的占用仲裁在单事务内完成，保证失败回滚、可重试、不会两边都占住。
        next_status, patch = self.rules.validate_transition(
            actor, entity, "reassign", payload, self._lookup
        )
        expected_task_version = (
            int(expected_version) if expected_version is not None else entity["version"]
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.reassign_task(
            task_id=entity["id"],
            expected_task_version=expected_task_version,
            expected_incident_version=int(payload["incident_version"]),
            new_team_id=payload["new_team_id"],
            data=merged,
            from_status=entity["status"],
            to_status=next_status,
        )
        audit_patch = {key: value for key, value in patch.items() if key != "reassignment_history"}
        self.audit.record(
            entity["id"],
            actor,
            "reassign",
            entity["status"],
            updated["status"],
            {"patch": audit_patch, "incident_version": int(payload["incident_version"])},
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

    def team_states(self):
        return self.repository.list_team_states()

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
