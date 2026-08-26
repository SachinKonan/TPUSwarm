"""Serializable contracts shared by TPUSwarm clients and controllers."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Any


def _json_dict(value: Mapping[str, Any], *, field_name: str) -> dict[str, Any]:
    result = dict(value)
    try:
        json.dumps(result, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be JSON serializable") from exc
    return result


class Priority(IntEnum):
    """SkyPilot-compatible priorities; larger values launch first."""

    SPECULATIVE = 100
    NORMAL = 500
    WORKFLOW_START = 600
    ORDINARY_RECOVERY = 800
    BLOCKING_RECOVERY = 900


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELLED}

    @property
    def occupies_slot(self) -> bool:
        return self in {self.SUBMITTING, self.SUBMITTED, self.RUNNING}


class ConsistencyMode(StrEnum):
    ALL_AT_BARRIER = "ALL_AT_BARRIER"
    INDEPENDENT = "INDEPENDENT"


class WorkflowStatus(StrEnum):
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class TaskSpec:
    """One logical task whose process lifecycle is delegated to SkyPilot."""

    kind: str
    resource_class: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: Priority = Priority.NORMAL
    recovery_priority: Priority = Priority.BLOCKING_RECOVERY
    idempotency_key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("kind must not be empty")
        if not self.resource_class.strip():
            raise ValueError("resource_class must not be empty")
        if not -1000 <= int(self.priority) <= 1000:
            raise ValueError("priority must be between -1000 and 1000")
        object.__setattr__(
            self, "payload", _json_dict(self.payload, field_name="payload")
        )
        object.__setattr__(
            self, "metadata", _json_dict(self.metadata, field_name="metadata")
        )


@dataclass(frozen=True)
class ManagedJobSpec:
    """Trusted SkyPilot job definition emitted by an AutoResumable handler."""

    name: str
    run: str
    resources: Mapping[str, Any]
    pool: str | None = None
    setup: str | None = None
    workdir: str | Mapping[str, Any] | None = None
    num_nodes: int = 1
    envs: Mapping[str, str] = field(default_factory=dict)
    secrets: Mapping[str, str | None] = field(default_factory=dict)
    file_mounts: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)
    api_server_access: bool = True

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.run.strip():
            raise ValueError("managed job name and run command must not be empty")
        if self.num_nodes < 1:
            raise ValueError("num_nodes must be positive")
        if self.pool is not None and (
            self.setup is not None or self.workdir is not None or self.file_mounts
        ):
            raise ValueError(
                "pool jobs cannot define setup, workdir, or file_mounts; "
                "put them in the pool configuration"
            )
        object.__setattr__(
            self, "resources", _json_dict(self.resources, field_name="resources")
        )
        object.__setattr__(self, "envs", _json_dict(self.envs, field_name="envs"))
        secrets = _json_dict(self.secrets, field_name="secrets")
        resolved_secrets: dict[str, str] = {}
        for name, value in secrets.items():
            if value is None:
                try:
                    value = os.environ[name]
                except KeyError as exc:
                    raise ValueError(
                        f"secret {name!r} is null but is not set in the "
                        "TPUSwarm controller environment"
                    ) from exc
            if not isinstance(value, str):
                raise TypeError(f"secret {name!r} must be a string or null")
            resolved_secrets[name] = value
        object.__setattr__(self, "secrets", resolved_secrets)
        object.__setattr__(
            self,
            "file_mounts",
            _json_dict(self.file_mounts, field_name="file_mounts"),
        )
        object.__setattr__(self, "config", _json_dict(self.config, field_name="config"))

    def to_sky_config(self, *, priority: Priority) -> dict[str, Any]:
        resources = dict(self.resources)
        resources["priority"] = int(priority)
        result: dict[str, Any] = {
            "name": self.name,
            "run": self.run,
            "resources": resources,
            "num_nodes": self.num_nodes,
            "api_server_access": self.api_server_access,
        }
        for key, value in (
            ("setup", self.setup),
            ("workdir", self.workdir),
            ("envs", dict(self.envs) or None),
            ("secrets", dict(self.secrets) or None),
            ("file_mounts", dict(self.file_mounts) or None),
            ("config", dict(self.config) or None),
        ):
            if value is not None:
                result[key] = value
        return result


@dataclass(frozen=True)
class CheckpointRef:
    uri: str
    sequence: int
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TaskRecord:
    task_id: str
    kind: str
    resource_class: str
    payload: Mapping[str, Any]
    metadata: Mapping[str, Any]
    status: TaskStatus
    priority: Priority
    recovery_priority: Priority
    idempotency_key: str | None
    job_id: str | None
    job_name: str
    executor_status: str | None
    recovery_count: int
    checkpoint: CheckpointRef | None
    result: Mapping[str, Any] | None
    last_error: str | None
    workflow_id: str | None
    component_key: str | None
    generation: int | None
    submission_started_at: float | None
    created_at: float
    updated_at: float
    completed_at: float | None

    def as_spec(self) -> TaskSpec:
        return TaskSpec(
            task_id=self.task_id,
            kind=self.kind,
            resource_class=self.resource_class,
            payload=self.payload,
            priority=self.priority,
            recovery_priority=self.recovery_priority,
            idempotency_key=self.idempotency_key,
            metadata=self.metadata,
        )


@dataclass(frozen=True)
class ComponentSpec:
    key: str
    task: TaskSpec
    generation: int = 0
    required: bool = True

    def __post_init__(self) -> None:
        if not self.key.strip():
            raise ValueError("component key must not be empty")
        if self.generation < 0:
            raise ValueError("component generation must be non-negative")


@dataclass(frozen=True)
class WorkflowSpec:
    kind: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    workflow_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    consistency_mode: ConsistencyMode = ConsistencyMode.INDEPENDENT
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("kind must not be empty")
        object.__setattr__(
            self, "payload", _json_dict(self.payload, field_name="payload")
        )
        object.__setattr__(
            self, "metadata", _json_dict(self.metadata, field_name="metadata")
        )


@dataclass(frozen=True)
class WorkflowRecord:
    workflow_id: str
    kind: str
    payload: Mapping[str, Any]
    metadata: Mapping[str, Any]
    state: Mapping[str, Any]
    status: WorkflowStatus
    consistency_mode: ConsistencyMode
    event_cursor: int
    result: Mapping[str, Any] | None
    last_error: str | None
    created_at: float
    updated_at: float
    completed_at: float | None


@dataclass(frozen=True)
class WorkflowEvent:
    event_id: int
    workflow_id: str
    kind: str
    component_key: str | None
    generation: int | None
    payload: Mapping[str, Any]
    created_at: float


@dataclass(frozen=True)
class BarrierCommit:
    workflow_id: str
    barrier: int
    controller_checkpoint_uri: str | None
    components: Mapping[str, CheckpointRef]
    created_at: float


@dataclass(frozen=True)
class ResourcePolicy:
    """Limits admitted jobs while retaining warm pool capacity for recovery."""

    resource_class: str
    target_workers: int
    recovery_reserve: int = 0

    def __post_init__(self) -> None:
        if self.target_workers < 1:
            raise ValueError("target_workers must be positive")
        if not 0 <= self.recovery_reserve < self.target_workers:
            raise ValueError("recovery_reserve must be in [0, target_workers)")


@dataclass(frozen=True)
class ExecutorJob:
    job_id: str
    name: str
    status: str
    recovery_count: int = 0
    failure_reason: str | None = None

    @property
    def is_terminal(self) -> bool:
        state = self.status.upper()
        return (
            state == "SUCCEEDED"
            or state.startswith("FAILED")
            or state
            in {
                "CANCELLED",
                "CANCELLING",
            }
        )


@dataclass(frozen=True)
class ControllerLease:
    controller_id: str
    fence_token: int
    lease_expires_at: float
