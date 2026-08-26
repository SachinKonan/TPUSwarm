"""Trusted task compilers and durable workflow reducers."""

from __future__ import annotations

import abc
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, Protocol

from tpuswarm.types import (
    ComponentSpec,
    ManagedJobSpec,
    TaskRecord,
    WorkflowEvent,
    WorkflowRecord,
    WorkflowSpec,
    WorkflowStatus,
)


class WorkflowStore(Protocol):
    def submit_workflow(
        self,
        spec: WorkflowSpec,
        components: Sequence[ComponentSpec],
        *,
        initial_state: Mapping[str, Any],
    ) -> WorkflowRecord: ...

    def get_workflow(self, workflow_id: str) -> WorkflowRecord: ...

    def get_workflow_events(
        self, workflow_id: str, *, after: int = 0
    ) -> list[WorkflowEvent]: ...

    def get_workflow_components(self, workflow_id: str) -> dict[str, TaskRecord]: ...

    def replace_component(
        self, workflow_id: str, component: ComponentSpec
    ) -> TaskRecord: ...

    def save_workflow_state(
        self, workflow_id: str, state: Mapping[str, Any], event_cursor: int
    ) -> WorkflowRecord: ...

    def complete_workflow(
        self, workflow_id: str, *, result: Mapping[str, Any]
    ) -> WorkflowRecord: ...

    def fail_workflow(self, workflow_id: str, *, reason: str) -> WorkflowRecord: ...


class AutoResumable(abc.ABC):
    """Compiles one logical leaf into a native SkyPilot Managed Job.

    SkyPilot owns process restart, spot recovery, failover, and cleanup. The
    handler owns the application command, checkpoint location, completion
    probe, lifecycle hooks, and resource/pool selection.
    """

    @classmethod
    def validate_payload(cls, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        return dict(payload)

    @abc.abstractmethod
    def managed_job(self, task: TaskRecord) -> ManagedJobSpec:
        """Returns a trusted job definition for the immutable task record."""


class MultiAutoResumable(abc.ABC):
    """A durable reducer for dynamic child jobs and algorithmic barriers."""

    @classmethod
    def validate_payload(cls, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        return dict(payload)

    @abc.abstractmethod
    def initial_state(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        pass

    @abc.abstractmethod
    def reduce(
        self, state: Mapping[str, Any], event: WorkflowEvent
    ) -> Mapping[str, Any]:
        pass

    @abc.abstractmethod
    def desired_components(
        self, workflow: WorkflowRecord, state: Mapping[str, Any]
    ) -> Sequence[ComponentSpec]:
        pass

    def completion_result(
        self,
        workflow: WorkflowRecord,
        state: Mapping[str, Any],
        components: Mapping[str, TaskRecord],
    ) -> Mapping[str, Any] | None:
        return None

    def failure_reason(
        self,
        workflow: WorkflowRecord,
        state: Mapping[str, Any],
        components: Mapping[str, TaskRecord],
    ) -> str | None:
        return None


class TaskRegistry:
    """Server-side mapping from stable task kinds to trusted code."""

    def __init__(self) -> None:
        self._tasks: dict[str, type[AutoResumable]] = {}
        self._workflows: dict[str, type[MultiAutoResumable]] = {}

    def register_task(
        self, kind: str, handler: type[AutoResumable]
    ) -> type[AutoResumable]:
        if not inspect.isclass(handler) or not issubclass(handler, AutoResumable):
            raise TypeError("task handler must be an AutoResumable class")
        if kind in self._tasks:
            raise ValueError(f"task kind is already registered: {kind}")
        self._tasks[kind] = handler
        return handler

    def register_workflow(
        self, kind: str, handler: type[MultiAutoResumable]
    ) -> type[MultiAutoResumable]:
        if not inspect.isclass(handler) or not issubclass(handler, MultiAutoResumable):
            raise TypeError("workflow handler must be a MultiAutoResumable class")
        if kind in self._workflows:
            raise ValueError(f"workflow kind is already registered: {kind}")
        self._workflows[kind] = handler
        return handler

    def task(self, kind: str) -> AutoResumable:
        try:
            return self._tasks[kind]()
        except KeyError as exc:
            raise KeyError(f"unknown task kind: {kind}") from exc

    def workflow(self, kind: str) -> MultiAutoResumable:
        try:
            return self._workflows[kind]()
        except KeyError as exc:
            raise KeyError(f"unknown workflow kind: {kind}") from exc

    def validate_task(self, kind: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            handler = self._tasks[kind]
        except KeyError as exc:
            raise KeyError(f"unknown task kind: {kind}") from exc
        return handler.validate_payload(payload)

    def validate_workflow(
        self, kind: str, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        try:
            handler = self._workflows[kind]
        except KeyError as exc:
            raise KeyError(f"unknown workflow kind: {kind}") from exc
        return handler.validate_payload(payload)


class WorkflowEngine:
    """Replays workflow events and materializes only desired child generations."""

    def __init__(self, store: WorkflowStore, registry: TaskRegistry):
        self.store = store
        self.registry = registry

    def _validated_component(self, component: ComponentSpec) -> ComponentSpec:
        payload = self.registry.validate_task(
            component.task.kind, component.task.payload
        )
        return replace(component, task=replace(component.task, payload=payload))

    def submit(self, spec: WorkflowSpec) -> WorkflowRecord:
        handler = self.registry.workflow(spec.kind)
        payload = handler.validate_payload(spec.payload)
        spec = replace(spec, payload=payload)
        placeholder = WorkflowRecord(
            workflow_id=spec.workflow_id,
            kind=spec.kind,
            payload=spec.payload,
            metadata=spec.metadata,
            state=handler.initial_state(spec.payload),
            status=WorkflowStatus.ACTIVE,
            consistency_mode=spec.consistency_mode,
            event_cursor=0,
            result=None,
            last_error=None,
            created_at=0,
            updated_at=0,
            completed_at=None,
        )
        components = [
            self._validated_component(component)
            for component in handler.desired_components(placeholder, placeholder.state)
        ]
        return self.store.submit_workflow(
            spec, components, initial_state=placeholder.state
        )

    def reconcile(self, workflow_id: str) -> WorkflowRecord:
        workflow = self.store.get_workflow(workflow_id)
        if workflow.status is not WorkflowStatus.ACTIVE:
            return workflow
        handler = self.registry.workflow(workflow.kind)
        state: Mapping[str, Any] = workflow.state
        cursor = workflow.event_cursor
        for event in self.store.get_workflow_events(workflow_id, after=cursor):
            state = handler.reduce(state, event)
            cursor = event.event_id

        projected = replace(workflow, state=state, event_cursor=cursor)
        current = self.store.get_workflow_components(workflow_id)
        for desired in (
            self._validated_component(component)
            for component in handler.desired_components(projected, state)
        ):
            existing = current.get(desired.key)
            if existing is None or desired.generation > (existing.generation or 0):
                self.store.replace_component(workflow_id, desired)

        workflow = self.store.save_workflow_state(workflow_id, state, cursor)
        components = self.store.get_workflow_components(workflow_id)
        result = handler.completion_result(workflow, state, components)
        if result is not None:
            return self.store.complete_workflow(workflow_id, result=result)
        reason = handler.failure_reason(workflow, state, components)
        if reason is not None:
            return self.store.fail_workflow(workflow_id, reason=reason)
        return workflow
