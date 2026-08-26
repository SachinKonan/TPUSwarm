"""Standard-library client for TPUSwarm's thin semantic API."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any

from tpuswarm.errors import BackendUnavailableError, SwarmError
from tpuswarm.serialization import task_record_from_dict, to_jsonable
from tpuswarm.types import CheckpointRef, TaskRecord, TaskSpec, WorkflowSpec


class SwarmClient:
    def __init__(
        self, base_url: str, *, bearer_token: str | None = None, timeout: float = 30
    ):
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self.timeout = timeout

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> Any:
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(to_jsonable(body)).encode()
        if self.bearer_token is not None:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise SwarmError(f"TPUSwarm returned HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise BackendUnavailableError(
                f"could not reach TPUSwarm at {self.base_url}: {exc.reason}"
            ) from exc
        return None if not payload else json.loads(payload)

    def submit_task(self, spec: TaskSpec) -> TaskRecord:
        return task_record_from_dict(
            self._request("POST", "/v1/tasks", to_jsonable(spec))
        )

    def get_task(self, task_id: str) -> TaskRecord:
        return task_record_from_dict(self._request("GET", f"/v1/tasks/{task_id}"))

    def submit_workflow(self, spec: WorkflowSpec) -> Mapping[str, Any]:
        return self._request("POST", "/v1/workflows", to_jsonable(spec))

    def publish_checkpoint(self, task_id: str, checkpoint: CheckpointRef) -> TaskRecord:
        return task_record_from_dict(
            self._request(
                "POST",
                f"/v1/tasks/{task_id}/checkpoints",
                to_jsonable(checkpoint),
            )
        )
