import pytest

from tpuswarm.errors import BarrierConflictError
from tpuswarm.store import SwarmStore
from tpuswarm.types import (
    CheckpointRef,
    ComponentSpec,
    ConsistencyMode,
    Priority,
    ResourcePolicy,
    TaskSpec,
    WorkflowSpec,
)


def test_admission_preserves_one_warm_recovery_slot(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    store.set_resource_policy(
        ResourcePolicy("v6e-8", target_workers=3, recovery_reserve=1), now=0
    )
    for task_id in ("normal-a", "normal-b", "normal-c"):
        store.submit_task(
            TaskSpec(task_id=task_id, kind="test", resource_class="v6e-8"), now=0
        )

    admitted = store.reserve_submissions(now=1)
    assert [task.task_id for task in admitted] == ["normal-a", "normal-b"]

    store.submit_task(
        TaskSpec(
            task_id="recovery",
            kind="test",
            resource_class="v6e-8",
            priority=Priority.BLOCKING_RECOVERY,
        ),
        now=2,
    )
    recovery = store.reserve_submissions(now=2)
    assert [task.task_id for task in recovery] == ["recovery"]


def test_component_checkpoints_commit_only_a_coherent_barrier(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    store.submit_workflow(
        WorkflowSpec(
            workflow_id="ensemble",
            kind="test",
            consistency_mode=ConsistencyMode.ALL_AT_BARRIER,
        ),
        [
            ComponentSpec(
                key=key,
                task=TaskSpec(
                    task_id=f"task-{key}", kind="model", resource_class="v6e-8"
                ),
            )
            for key in ("qwen", "gemma")
        ],
        initial_state={},
        now=0,
    )
    store.record_checkpoint(
        "task-qwen", CheckpointRef("gs://checkpoints/qwen/1", 1), now=1
    )

    with pytest.raises(BarrierConflictError, match="gemma"):
        store.commit_barrier("ensemble", 1, now=2)

    store.record_checkpoint(
        "task-gemma", CheckpointRef("gs://checkpoints/gemma/1", 1), now=3
    )
    commit = store.commit_barrier(
        "ensemble",
        1,
        controller_checkpoint_uri="gs://checkpoints/controller/1",
        now=4,
    )
    assert set(commit.components) == {"qwen", "gemma"}


def test_idempotency_key_returns_original_task(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    first = store.submit_task(
        TaskSpec(
            task_id="first",
            idempotency_key="request-1",
            kind="test",
            resource_class="v6e-8",
        )
    )
    second = store.submit_task(
        TaskSpec(
            task_id="second",
            idempotency_key="request-1",
            kind="test",
            resource_class="v6e-8",
        )
    )
    assert second.task_id == first.task_id


def test_executor_names_are_cloud_safe_and_collision_resistant():
    prefix = "same-prefix-" + "x" * 80
    first = SwarmStore.executor_name(prefix + "-a")
    second = SwarmStore.executor_name(prefix + "-b")

    assert first != second
    assert len(first) <= 63
    assert first == first.lower()
    assert set(first) <= set("abcdefghijklmnopqrstuvwxyz0123456789-")
