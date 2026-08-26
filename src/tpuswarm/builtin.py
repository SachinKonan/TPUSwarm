"""Generic handlers built on native SkyPilot jobs."""

from __future__ import annotations

import shlex
from collections.abc import Mapping, Sequence
from typing import Any

from tpuswarm.handlers import AutoResumable, MultiAutoResumable, TaskRegistry
from tpuswarm.types import (
    ComponentSpec,
    ManagedJobSpec,
    Priority,
    TaskRecord,
    TaskSpec,
    TaskStatus,
    WorkflowEvent,
    WorkflowRecord,
)


def _argv(value: Any, *, field_name: str, required: bool = True) -> list[str] | None:
    if value is None and not required:
        return None
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise ValueError(f"{field_name} must be a non-empty list of strings")
    return value


class CommandAutoResumable(AutoResumable):
    """Compiles a trusted argv command into a SkyPilot Managed Job."""

    @classmethod
    def validate_payload(cls, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        result = dict(payload)
        result["argv"] = _argv(result.get("argv"), field_name="argv")
        for key in ("completion_probe_argv", "preemption_argv"):
            result[key] = _argv(result.get(key), field_name=key, required=False)
            if result[key] is None:
                result.pop(key)
        resources = result.get("resources")
        if not isinstance(resources, dict) or not resources:
            raise ValueError("resources must be a non-empty mapping")
        for key in ("envs", "secrets", "file_mounts", "config"):
            value = result.get(key, {})
            if not isinstance(value, dict):
                raise TypeError(f"{key} must be a mapping")
        num_nodes = result.get("num_nodes", 1)
        if not isinstance(num_nodes, int) or num_nodes < 1:
            raise ValueError("num_nodes must be a positive integer")
        pool = result.get("pool")
        if pool is not None and (not isinstance(pool, str) or not pool):
            raise ValueError("pool must be a non-empty string")
        return result

    def managed_job(self, task: TaskRecord) -> ManagedJobSpec:
        payload = task.payload
        lines = ["set -euo pipefail"]
        probe = payload.get("completion_probe_argv")
        if probe is not None:
            lines.extend(
                [
                    f"if {shlex.join(probe)}; then",
                    "  echo 'TPUSwarm completion probe: already complete'",
                    "  exit 0",
                    "fi",
                ]
            )
        lines.append(f"exec {shlex.join(payload['argv'])}")

        config = dict(payload.get("config", {}))
        preemption = payload.get("preemption_argv")
        if preemption is not None:
            hooks = list(config.get("hooks", []))
            hooks.append(
                {
                    "run": shlex.join(preemption),
                    "events": ["preemption", "down"],
                    "timeout": int(payload.get("preemption_timeout", 90)),
                }
            )
            config["hooks"] = hooks

        envs = dict(payload.get("envs", {}))
        envs.update(
            {
                "TPUSWARM_TASK_ID": task.task_id,
                "TPUSWARM_JOB_NAME": task.job_name,
            }
        )
        return ManagedJobSpec(
            name=task.job_name,
            run="\n".join(lines),
            resources=payload["resources"],
            pool=payload.get("pool"),
            setup=payload.get("setup"),
            workdir=payload.get("workdir"),
            num_nodes=payload.get("num_nodes", 1),
            envs=envs,
            secrets=payload.get("secrets", {}),
            file_mounts=payload.get("file_mounts", {}),
            config=config,
            api_server_access=bool(payload.get("api_server_access", True)),
        )


class StaticMultiAutoResumable(MultiAutoResumable):
    """Coordinates fixed child jobs not expressible as one SkyPilot Job Group."""

    @classmethod
    def validate_payload(cls, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        components = payload.get("components")
        if not isinstance(components, list) or not components:
            raise ValueError("components must be a non-empty list")
        keys = [item.get("key") for item in components if isinstance(item, dict)]
        if len(keys) != len(components) or any(
            not isinstance(key, str) or not key for key in keys
        ):
            raise ValueError("each component must have a non-empty string key")
        if len(set(keys)) != len(keys):
            raise ValueError("component keys must be unique")
        return dict(payload)

    def initial_state(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"component_status": {}, "last_barrier": None}

    def reduce(
        self, state: Mapping[str, Any], event: WorkflowEvent
    ) -> Mapping[str, Any]:
        result = dict(state)
        statuses = dict(result.get("component_status", {}))
        if event.component_key is not None:
            statuses[event.component_key] = event.kind
        result["component_status"] = statuses
        if event.kind == "BarrierCommitted":
            result["last_barrier"] = event.payload["barrier"]
        return result

    def desired_components(
        self, workflow: WorkflowRecord, state: Mapping[str, Any]
    ) -> Sequence[ComponentSpec]:
        result = []
        for value in workflow.payload["components"]:
            task = value["task"]
            generation = int(value.get("generation", 0))
            result.append(
                ComponentSpec(
                    key=value["key"],
                    generation=generation,
                    required=bool(value.get("required", True)),
                    task=TaskSpec(
                        task_id=task.get(
                            "task_id",
                            f"{workflow.workflow_id}-{value['key']}-g{generation}",
                        ),
                        kind=task["kind"],
                        resource_class=task["resource_class"],
                        payload=task.get("payload", {}),
                        priority=Priority(
                            task.get("priority", Priority.WORKFLOW_START)
                        ),
                        recovery_priority=Priority(
                            task.get("recovery_priority", Priority.BLOCKING_RECOVERY)
                        ),
                        idempotency_key=task.get("idempotency_key"),
                        metadata=task.get("metadata", {}),
                    ),
                )
            )
        return result

    def completion_result(
        self,
        workflow: WorkflowRecord,
        state: Mapping[str, Any],
        components: Mapping[str, TaskRecord],
    ) -> Mapping[str, Any] | None:
        if components and all(
            component.status is TaskStatus.SUCCEEDED
            for component in components.values()
        ):
            return {
                "components": {
                    key: component.result or {} for key, component in components.items()
                },
                "last_barrier": state.get("last_barrier"),
            }
        return None

    def failure_reason(
        self,
        workflow: WorkflowRecord,
        state: Mapping[str, Any],
        components: Mapping[str, TaskRecord],
    ) -> str | None:
        failed = [
            key
            for key, component in components.items()
            if component.status is TaskStatus.FAILED
        ]
        return (
            f"components failed after SkyPilot recovery: {', '.join(sorted(failed))}"
            if failed
            else None
        )


def register_builtin_handlers(registry: TaskRegistry) -> None:
    registry.register_task("command.v1", CommandAutoResumable)
    registry.register_workflow("static_multi.v1", StaticMultiAutoResumable)
