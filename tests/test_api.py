from fastapi.testclient import TestClient

from tpuswarm.api import create_app
from tpuswarm.builtin import register_builtin_handlers
from tpuswarm.controller import TPUSwarmController
from tpuswarm.handlers import TaskRegistry
from tpuswarm.store import SwarmStore


class EmptyBackend:
    async def list_jobs(self):
        return []

    async def launch(self, job, *, priority):
        raise AssertionError("controller should not launch during this API test")


def test_api_auth_and_idempotent_submission(tmp_path):
    store = SwarmStore(tmp_path / "swarm.db")
    registry = TaskRegistry()
    register_builtin_handlers(registry)
    controller = TPUSwarmController(store, registry, EmptyBackend())
    app = create_app(store, registry, controller, bearer_token="secret")
    body = {
        "task_id": "task",
        "idempotency_key": "request-1",
        "kind": "command.v1",
        "resource_class": "v6e",
        "payload": {
            "argv": ["true"],
            "resources": {"accelerators": "tpu-v6e-8"},
        },
    }

    with TestClient(app) as client:
        assert client.post("/v1/tasks", json=body).status_code == 401
        response = client.post(
            "/v1/tasks", headers={"Authorization": "Bearer secret"}, json=body
        )
        assert response.status_code == 200
        assert response.json()["status"] == "PENDING"
