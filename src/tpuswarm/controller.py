"""Restartable reconciliation over durable workflow state and SkyPilot jobs."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass

from tpuswarm.backend import JobBackend
from tpuswarm.errors import BackendUnavailableError, LeadershipLostError
from tpuswarm.handlers import TaskRegistry, WorkflowEngine
from tpuswarm.store import SwarmStore
from tpuswarm.types import ControllerLease, TaskStatus, WorkflowStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReconcileResult:
    is_leader: bool
    backend_available: bool
    adopted_tasks: tuple[str, ...]
    observed_tasks: tuple[str, ...]
    submitted_tasks: tuple[str, ...]
    reconciled_workflows: tuple[str, ...]


class TPUSwarmController:
    """Admits logical tasks and delegates every process attempt to SkyPilot."""

    def __init__(
        self,
        store: SwarmStore,
        registry: TaskRegistry,
        backend: JobBackend,
        *,
        controller_id: str | None = None,
        leadership_lease_seconds: float = 30,
        submission_grace_seconds: float = 120,
        reconcile_seconds: float = 10,
        submission_batch_size: int = 32,
    ) -> None:
        self.store = store
        self.registry = registry
        self.backend = backend
        self.workflow_engine = WorkflowEngine(store, registry)
        self.controller_id = controller_id or f"controller-{uuid.uuid4()}"
        self.leadership_lease_seconds = leadership_lease_seconds
        self.submission_grace_seconds = submission_grace_seconds
        self.reconcile_seconds = reconcile_seconds
        self.submission_batch_size = submission_batch_size
        self.lease: ControllerLease | None = None
        self.stopping = asyncio.Event()

    def start(self, *, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        if self.lease is not None and self.lease.lease_expires_at >= now:
            return True
        self.lease = self.store.acquire_leadership(
            self.controller_id,
            lease_seconds=self.leadership_lease_seconds,
            now=now,
        )
        return self.lease is not None

    async def reconcile_once(self, *, now: float | None = None) -> ReconcileResult:
        now = time.time() if now is None else now
        if self.lease is None and not self.start(now=now):
            return ReconcileResult(False, True, (), (), (), ())
        assert self.lease is not None
        try:
            self.lease = self.store.renew_leadership(
                self.lease,
                lease_seconds=self.leadership_lease_seconds,
                now=now,
            )
        except LeadershipLostError:
            self.lease = None
            return ReconcileResult(False, True, (), (), (), ())

        try:
            jobs = await self.backend.list_jobs()
        except BackendUnavailableError:
            logger.exception("SkyPilot is unavailable; preserving durable state")
            return ReconcileResult(True, False, (), (), (), ())

        by_id = {job.job_id: job for job in jobs}
        by_name = {job.name: job for job in jobs}
        adopted: list[str] = []
        observed: list[str] = []
        for task in self.store.list_tasks():
            if task.status is TaskStatus.SUBMITTING:
                job = by_name.get(task.job_name)
                if job is not None:
                    self.store.bind_job(
                        task.task_id,
                        job.job_id,
                        executor_status=job.status,
                        now=now,
                    )
                    adopted.append(task.task_id)
                    task = self.store.get_task(task.task_id)
                elif (
                    task.submission_started_at is not None
                    and now - task.submission_started_at
                    >= self.submission_grace_seconds
                ):
                    self.store.reset_submission(
                        task.task_id,
                        reason="no matching SkyPilot job found after submission grace",
                        now=now,
                    )
                    continue
            if task.job_id is not None and not task.status.is_terminal:
                job = by_id.get(task.job_id) or by_name.get(task.job_name)
                if job is not None:
                    self.store.observe_job(task.task_id, job, now=now)
                    observed.append(task.task_id)

        reconciled: list[str] = []
        for workflow in self.store.list_workflows(status=WorkflowStatus.ACTIVE):
            try:
                self.workflow_engine.reconcile(workflow.workflow_id)
                reconciled.append(workflow.workflow_id)
            except Exception:
                logger.exception(
                    "Workflow reconciliation failed: %s", workflow.workflow_id
                )

        submitted: list[str] = []
        for task in self.store.reserve_submissions(
            limit=self.submission_batch_size, now=now
        ):
            try:
                job_spec = self.registry.task(task.kind).managed_job(task)
            except Exception as exc:  # noqa: BLE001 - handler is a plugin boundary
                self.store.fail_task(
                    task.task_id,
                    reason=f"invalid managed-job definition: {type(exc).__name__}: {exc}",
                    now=now,
                )
                continue
            try:
                job = await self.backend.launch(job_spec, priority=task.priority)
            except BackendUnavailableError as exc:
                self.store.reset_submission(task.task_id, reason=str(exc), now=now)
                break
            self.store.bind_job(
                task.task_id,
                job.job_id,
                executor_status=job.status,
                now=now,
            )
            submitted.append(task.task_id)

        return ReconcileResult(
            True,
            True,
            tuple(sorted(adopted)),
            tuple(sorted(observed)),
            tuple(sorted(submitted)),
            tuple(sorted(reconciled)),
        )

    async def run_forever(self) -> None:
        while not self.stopping.is_set():
            await self.reconcile_once()
            try:
                await asyncio.wait_for(
                    self.stopping.wait(), timeout=self.reconcile_seconds
                )
            except TimeoutError:
                pass

    def stop(self) -> None:
        self.stopping.set()
