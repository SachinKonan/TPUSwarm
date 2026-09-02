"""Executor boundary and the native SkyPilot Managed Jobs adapter."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from tpuswarm.errors import BackendUnavailableError
from tpuswarm.types import ExecutorJob, ManagedJobSpec, Priority


class JobBackend(Protocol):
    async def launch(
        self, job: ManagedJobSpec, *, priority: Priority
    ) -> ExecutorJob: ...

    async def list_jobs(self) -> list[ExecutorJob]: ...


class SkyPilotBackend:
    """Submits and observes jobs through the configured SkyPilot API server."""

    async def launch(self, job: ManagedJobSpec, *, priority: Priority) -> ExecutorJob:
        return await asyncio.to_thread(self._launch, job, priority)

    async def list_jobs(self) -> list[ExecutorJob]:
        return await asyncio.to_thread(self._list_jobs)

    @staticmethod
    def _sky() -> Any:
        try:
            import sky
        except ImportError as exc:
            raise RuntimeError(
                "SkyPilot is required; install TPUSwarm with its 'skypilot' extra"
            ) from exc
        return sky

    @staticmethod
    def _record_dict(record: Any) -> dict[str, Any]:
        if isinstance(record, dict):
            return record
        if hasattr(record, "model_dump"):
            return record.model_dump()
        return dict(record)

    @staticmethod
    def _status(value: Any) -> str:
        return str(getattr(value, "value", value))

    def _launch(self, job: ManagedJobSpec, priority: Priority) -> ExecutorJob:
        sky = self._sky()
        try:
            task = sky.Task.from_yaml_config(job.to_sky_config(priority=priority))
            request_id = sky.jobs.launch(task, name=job.name, pool=job.pool)
            response = sky.stream_and_get(request_id)
        except Exception as exc:
            raise BackendUnavailableError(f"SkyPilot launch failed: {exc}") from exc
        job_ids = response[0] if isinstance(response, tuple) else response
        if not job_ids:
            raise BackendUnavailableError("SkyPilot did not return a managed job ID")
        return ExecutorJob(job_id=str(job_ids[0]), name=job.name, status="PENDING")

    def _list_jobs(self) -> list[ExecutorJob]:
        sky = self._sky()
        try:
            request_id = sky.jobs.queue_v2(
                refresh=True,
                skip_finished=False,
                all_users=False,
                fields=None,
            )
            response = sky.get(request_id)
        except sky.exceptions.ClusterDoesNotExist:
            # A fresh SkyPilot installation has no Managed Jobs controller
            # until the first job is launched. Treat that bootstrap state as
            # an empty queue so reconciliation can submit that first job.
            return []
        except Exception as exc:
            raise BackendUnavailableError(
                f"SkyPilot queue lookup failed: {exc}"
            ) from exc
        records = response[0] if isinstance(response, tuple) else response
        result = []
        for raw in records:
            record = self._record_dict(raw)
            failure_reason = record.get("failure_reason") or record.get(
                "failure_message"
            )
            result.append(
                ExecutorJob(
                    job_id=str(record["job_id"]),
                    name=str(record.get("job_name") or record.get("name")),
                    status=self._status(record.get("status")),
                    recovery_count=int(record.get("recovery_count") or 0),
                    failure_reason=(
                        None if failure_reason is None else str(failure_reason)
                    ),
                )
            )
        return result
