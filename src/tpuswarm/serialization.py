"""JSON conversion helpers for the control-plane boundary."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from tpuswarm.types import Priority, TaskRecord, TaskSpec, TaskStatus


def to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return to_jsonable(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {key: to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    return value


def task_spec_from_dict(value: Mapping[str, Any]) -> TaskSpec:
    fields = dict(value)
    fields["priority"] = Priority(fields.get("priority", Priority.NORMAL))
    fields["recovery_priority"] = Priority(
        fields.get("recovery_priority", Priority.BLOCKING_RECOVERY)
    )
    return TaskSpec(**fields)


def task_record_from_dict(value: Mapping[str, Any]) -> TaskRecord:
    fields = dict(value)
    fields["status"] = TaskStatus(fields["status"])
    fields["priority"] = Priority(fields["priority"])
    fields["recovery_priority"] = Priority(fields["recovery_priority"])
    checkpoint = fields.get("checkpoint")
    if checkpoint is not None:
        from tpuswarm.types import CheckpointRef

        fields["checkpoint"] = CheckpointRef(**checkpoint)
    return TaskRecord(**fields)
