import pytest

from tpuswarm.controller import TPUSwarmController
from tpuswarm.handlers import AutoResumable, TaskRegistry
from tpuswarm.store import SwarmStore
from tpuswarm.types import (
    ExecutorJob,
    ManagedJobSpec,
    Priority,
    TaskRecord,
    TaskSpec,
    TaskStatus,
)


class TestTask(AutoResumable):
    def managed_job(self, task: TaskRecord) -> ManagedJobSpec:
        return ManagedJobSpec(
            name=task.job_name,
            run="python train.py",
            resources={"accelerators": "tpu-v6e-8", "use_spot": True},
            pool="v6e-pool",
        )


class FakeBackend:
    def __init__(self):
        self.jobs: dict[str, ExecutorJob] = {}
        self.launches: list[tuple[ManagedJobSpec, Priority]] = []

    async def launch(self, job: ManagedJobSpec, *, priority: Priority) -> ExecutorJob:
        self.launches.append((job, priority))
        result = ExecutorJob(str(len(self.launches)), job.name, "PENDING")
        self.jobs[result.job_id] = result
        return result

    async def list_jobs(self) -> list[ExecutorJob]:
        return list(self.jobs.values())


def registry() -> TaskRegistry:
    result = TaskRegistry()
    result.register_task("test", TestTask)
    return result


@pytest.mark.asyncio
async def test_skypilot_owns_recovery_without_logical_resubmission(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    store.submit_task(TaskSpec(task_id="task", kind="test", resource_class="v6e"))
    backend = FakeBackend()
    controller = TPUSwarmController(store, registry(), backend, controller_id="one")

    first = await controller.reconcile_once(now=0)
    assert first.submitted_tasks == ("task",)
    assert len(backend.launches) == 1
    backend.jobs["1"] = ExecutorJob("1", "tpuswarm-task", "RECOVERING", 1)

    await controller.reconcile_once(now=1)
    record = store.get_task("task")
    assert record.status is TaskStatus.RUNNING
    assert record.recovery_count == 1
    assert len(backend.launches) == 1

    backend.jobs["1"] = ExecutorJob("1", "tpuswarm-task", "SUCCEEDED", 1)
    await controller.reconcile_once(now=2)
    assert store.get_task("task").status is TaskStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_restarted_controller_adopts_job_by_stable_name(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    store.submit_task(TaskSpec(task_id="task", kind="test", resource_class="v6e"))
    [reserved] = store.reserve_submissions(now=1)
    backend = FakeBackend()
    backend.jobs["77"] = ExecutorJob("77", reserved.job_name, "RUNNING")
    controller = TPUSwarmController(
        store, registry(), backend, controller_id="replacement"
    )

    result = await controller.reconcile_once(now=2)

    assert result.adopted_tasks == ("task",)
    assert store.get_task("task").job_id == "77"
    assert len(backend.launches) == 0
