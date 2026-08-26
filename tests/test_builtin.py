from tpuswarm.builtin import CommandAutoResumable
from tpuswarm.store import SwarmStore
from tpuswarm.types import Priority, TaskSpec


def test_command_handler_compiles_native_recovery_and_lifecycle_hook(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    record = store.submit_task(
        TaskSpec(
            task_id="train",
            kind="command.v1",
            resource_class="v6e-8",
            payload={
                "argv": ["python", "train.py"],
                "completion_probe_argv": ["test", "-f", "/checkpoints/DONE"],
                "preemption_argv": ["bash", "sync.sh"],
                "resources": {
                    "accelerators": "tpu-v6e-8",
                    "use_spot": True,
                    "job_recovery": {"strategy": "EAGER_NEXT_REGION"},
                },
                "pool": "v6e-pool",
            },
        )
    )
    payload = CommandAutoResumable.validate_payload(record.payload)
    record = record.__class__(**{**record.__dict__, "payload": payload})
    job = CommandAutoResumable().managed_job(record)

    assert job.pool == "v6e-pool"
    assert "completion probe" in job.run
    assert job.config["hooks"][0]["events"] == ["preemption", "down"]
    assert (
        job.to_sky_config(priority=Priority.BLOCKING_RECOVERY)["resources"]["priority"]
        == 900
    )
