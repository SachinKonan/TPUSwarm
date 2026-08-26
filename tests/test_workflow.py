from tpuswarm.builtin import register_builtin_handlers
from tpuswarm.handlers import TaskRegistry, WorkflowEngine
from tpuswarm.store import SwarmStore
from tpuswarm.types import ExecutorJob, WorkflowSpec, WorkflowStatus


def test_static_workflow_reduces_native_job_completion(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    registry = TaskRegistry()
    register_builtin_handlers(registry)
    engine = WorkflowEngine(store, registry)
    workflow = engine.submit(
        WorkflowSpec(
            workflow_id="workflow",
            kind="static_multi.v1",
            payload={
                "components": [
                    {
                        "key": key,
                        "task": {
                            "task_id": f"task-{key}",
                            "kind": "command.v1",
                            "resource_class": "v6e",
                            "payload": {
                                "argv": ["true"],
                                "resources": {"accelerators": "tpu-v6e-8"},
                            },
                        },
                    }
                    for key in ("a", "b")
                ]
            },
        )
    )
    assert workflow.status is WorkflowStatus.ACTIVE

    for index, task in enumerate(store.reserve_submissions(), start=1):
        store.bind_job(task.task_id, str(index))
        store.observe_job(
            task.task_id,
            ExecutorJob(str(index), task.job_name, "SUCCEEDED"),
        )

    completed = engine.reconcile("workflow")
    assert completed.status is WorkflowStatus.SUCCEEDED
    assert set(completed.result["components"]) == {"a", "b"}
