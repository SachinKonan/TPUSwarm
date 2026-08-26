"""Thin API for idempotent logical submissions and workflow state."""

from __future__ import annotations

import asyncio
import contextlib
import secrets
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from tpuswarm.controller import TPUSwarmController
from tpuswarm.errors import (
    BarrierConflictError,
    TaskNotFoundError,
    WorkflowNotFoundError,
)
from tpuswarm.handlers import TaskRegistry, WorkflowEngine
from tpuswarm.serialization import to_jsonable
from tpuswarm.store import SwarmStore
from tpuswarm.types import (
    CheckpointRef,
    ConsistencyMode,
    Priority,
    ResourcePolicy,
    TaskSpec,
    TaskStatus,
    WorkflowSpec,
    WorkflowStatus,
)


class SubmitTaskRequest(BaseModel):
    kind: str
    resource_class: str
    payload: dict[str, Any] = Field(default_factory=dict)
    task_id: str | None = None
    priority: Priority = Priority.NORMAL
    recovery_priority: Priority = Priority.BLOCKING_RECOVERY
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SubmitWorkflowRequest(BaseModel):
    kind: str
    payload: dict[str, Any] = Field(default_factory=dict)
    workflow_id: str | None = None
    consistency_mode: ConsistencyMode = ConsistencyMode.INDEPENDENT
    metadata: dict[str, Any] = Field(default_factory=dict)


class CheckpointRequest(BaseModel):
    uri: str
    sequence: int
    metadata: dict[str, Any] = Field(default_factory=dict)


class BarrierRequest(BaseModel):
    barrier: int
    controller_checkpoint_uri: str | None = None


class ResourcePolicyRequest(BaseModel):
    target_workers: int
    recovery_reserve: int = 0


def create_app(
    store: SwarmStore,
    registry: TaskRegistry,
    controller: TPUSwarmController,
    *,
    bearer_token: str | None = None,
) -> FastAPI:
    """Builds the small semantic layer in front of SkyPilot's API server."""

    workflow_engine = WorkflowEngine(store, registry)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        controller_task = asyncio.create_task(controller.run_forever())
        try:
            yield
        finally:
            controller.stop()
            controller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await controller_task

    app = FastAPI(title="TPUSwarm", version="0.1.0", lifespan=lifespan)

    def authenticate(authorization: str | None = Header(default=None)) -> None:
        if bearer_token is None:
            return
        scheme, _, credential = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(
            credential, bearer_token
        ):
            raise HTTPException(status_code=401, detail="invalid bearer token")

    auth = [Depends(authenticate)]

    @app.exception_handler(TaskNotFoundError)
    async def task_not_found(_, exc: TaskNotFoundError):
        return Response(content=str(exc), status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(WorkflowNotFoundError)
    async def workflow_not_found(_, exc: WorkflowNotFoundError):
        return Response(content=str(exc), status_code=status.HTTP_404_NOT_FOUND)

    @app.exception_handler(BarrierConflictError)
    async def barrier_conflict(_, exc: BarrierConflictError):
        return Response(content=str(exc), status_code=status.HTTP_409_CONFLICT)

    @app.get("/healthz")
    def healthz() -> Mapping[str, Any]:
        return {
            "ok": True,
            "controller_id": controller.controller_id,
            "is_leader": controller.lease is not None,
        }

    @app.post("/v1/tasks", dependencies=auth)
    def submit_task(request: SubmitTaskRequest):
        try:
            payload = registry.validate_task(request.kind, request.payload)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        values = request.model_dump(exclude={"task_id"})
        values["payload"] = payload
        if request.task_id is not None:
            values["task_id"] = request.task_id
        return to_jsonable(store.submit_task(TaskSpec(**values)))

    @app.get("/v1/tasks/{task_id}", dependencies=auth)
    def get_task(task_id: str):
        return to_jsonable(store.get_task(task_id))

    @app.get("/v1/tasks", dependencies=auth)
    def list_tasks(
        task_status: Annotated[TaskStatus | None, Query(alias="status")] = None,
    ):
        return to_jsonable(store.list_tasks(status=task_status))

    @app.post("/v1/tasks/{task_id}/checkpoints", dependencies=auth)
    def checkpoint(task_id: str, request: CheckpointRequest):
        return to_jsonable(
            store.record_checkpoint(task_id, CheckpointRef(**request.model_dump()))
        )

    @app.post("/v1/workflows", dependencies=auth)
    def submit_workflow(request: SubmitWorkflowRequest):
        values = request.model_dump(exclude={"workflow_id"})
        if request.workflow_id is not None:
            values["workflow_id"] = request.workflow_id
        try:
            return to_jsonable(workflow_engine.submit(WorkflowSpec(**values)))
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/v1/workflows/{workflow_id}", dependencies=auth)
    def get_workflow(workflow_id: str):
        return {
            "workflow": to_jsonable(store.get_workflow(workflow_id)),
            "components": to_jsonable(store.get_workflow_components(workflow_id)),
            "latest_barrier": to_jsonable(store.latest_barrier(workflow_id)),
        }

    @app.get("/v1/workflows", dependencies=auth)
    def list_workflows(
        workflow_status: Annotated[WorkflowStatus | None, Query(alias="status")] = None,
    ):
        return to_jsonable(store.list_workflows(status=workflow_status))

    @app.post("/v1/workflows/{workflow_id}/barriers", dependencies=auth)
    def commit_barrier(workflow_id: str, request: BarrierRequest):
        return to_jsonable(
            store.commit_barrier(
                workflow_id,
                request.barrier,
                controller_checkpoint_uri=request.controller_checkpoint_uri,
            )
        )

    @app.put("/v1/resources/{resource_class}", dependencies=auth)
    def set_resource_policy(resource_class: str, request: ResourcePolicyRequest):
        policy = ResourcePolicy(resource_class=resource_class, **request.model_dump())
        store.set_resource_policy(policy)
        return to_jsonable(policy)

    return app
