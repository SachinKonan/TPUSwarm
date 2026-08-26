"""Transactional workflow and admission state for the SkyPilot-backed runtime."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from tpuswarm.errors import (
    BarrierConflictError,
    LeadershipLostError,
    TaskNotFoundError,
    WorkflowNotFoundError,
)
from tpuswarm.types import (
    BarrierCommit,
    CheckpointRef,
    ComponentSpec,
    ConsistencyMode,
    ControllerLease,
    ExecutorJob,
    Priority,
    ResourcePolicy,
    TaskRecord,
    TaskSpec,
    TaskStatus,
    WorkflowEvent,
    WorkflowRecord,
    WorkflowSpec,
    WorkflowStatus,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    kind TEXT NOT NULL,
    resource_class TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    recovery_priority INTEGER NOT NULL,
    job_id TEXT UNIQUE,
    job_name TEXT NOT NULL UNIQUE,
    executor_status TEXT,
    recovery_count INTEGER NOT NULL DEFAULT 0,
    checkpoint_uri TEXT,
    checkpoint_sequence INTEGER,
    checkpoint_metadata_json TEXT,
    result_json TEXT,
    last_error TEXT,
    workflow_id TEXT,
    component_key TEXT,
    generation INTEGER,
    submission_started_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL
);
CREATE INDEX IF NOT EXISTS tasks_admission_idx
    ON tasks(status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS tasks_resource_idx
    ON tasks(resource_class, status);
CREATE UNIQUE INDEX IF NOT EXISTS tasks_component_generation_idx
    ON tasks(workflow_id, component_key, generation)
    WHERE workflow_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS checkpoints (
    task_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    uri TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY(task_id, sequence),
    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
);

CREATE TABLE IF NOT EXISTS workflows (
    workflow_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    state_json TEXT NOT NULL,
    status TEXT NOT NULL,
    consistency_mode TEXT NOT NULL,
    event_cursor INTEGER NOT NULL DEFAULT 0,
    result_json TEXT,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL
);

CREATE TABLE IF NOT EXISTS workflow_components (
    workflow_id TEXT NOT NULL,
    component_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    task_id TEXT NOT NULL UNIQUE,
    required INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY(workflow_id, component_key),
    FOREIGN KEY(workflow_id) REFERENCES workflows(workflow_id),
    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
);

CREATE TABLE IF NOT EXISTS workflow_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    component_key TEXT,
    generation INTEGER,
    payload_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    FOREIGN KEY(workflow_id) REFERENCES workflows(workflow_id)
);
CREATE INDEX IF NOT EXISTS workflow_events_cursor_idx
    ON workflow_events(workflow_id, event_id);

CREATE TABLE IF NOT EXISTS workflow_commits (
    workflow_id TEXT NOT NULL,
    barrier INTEGER NOT NULL,
    controller_checkpoint_uri TEXT,
    manifest_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY(workflow_id, barrier),
    FOREIGN KEY(workflow_id) REFERENCES workflows(workflow_id)
);

CREATE TABLE IF NOT EXISTS resource_policies (
    resource_class TEXT PRIMARY KEY,
    target_workers INTEGER NOT NULL,
    recovery_reserve INTEGER NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS controller_leadership (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    controller_id TEXT NOT NULL,
    fence_token INTEGER NOT NULL,
    lease_expires_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
"""


class SwarmStore:
    """Durable task admission, executor mapping, and workflow event log."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(
                parents=True, exist_ok=True
            )
        with self._connect() as connection:
            connection.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        if self.path != ":memory:":
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @staticmethod
    def _dumps(value: Mapping[str, Any]) -> str:
        return json.dumps(dict(value), sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _loads(value: str | None) -> dict[str, Any] | None:
        return None if value is None else json.loads(value)

    def _task_from_row(self, row: sqlite3.Row) -> TaskRecord:
        checkpoint = None
        if row["checkpoint_uri"] is not None:
            checkpoint = CheckpointRef(
                uri=row["checkpoint_uri"],
                sequence=row["checkpoint_sequence"],
                metadata=self._loads(row["checkpoint_metadata_json"]) or {},
            )
        return TaskRecord(
            task_id=row["task_id"],
            kind=row["kind"],
            resource_class=row["resource_class"],
            payload=self._loads(row["payload_json"]) or {},
            metadata=self._loads(row["metadata_json"]) or {},
            status=TaskStatus(row["status"]),
            priority=Priority(row["priority"]),
            recovery_priority=Priority(row["recovery_priority"]),
            idempotency_key=row["idempotency_key"],
            job_id=row["job_id"],
            job_name=row["job_name"],
            executor_status=row["executor_status"],
            recovery_count=row["recovery_count"],
            checkpoint=checkpoint,
            result=self._loads(row["result_json"]),
            last_error=row["last_error"],
            workflow_id=row["workflow_id"],
            component_key=row["component_key"],
            generation=row["generation"],
            submission_started_at=row["submission_started_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )

    def _workflow_from_row(self, row: sqlite3.Row) -> WorkflowRecord:
        return WorkflowRecord(
            workflow_id=row["workflow_id"],
            kind=row["kind"],
            payload=self._loads(row["payload_json"]) or {},
            metadata=self._loads(row["metadata_json"]) or {},
            state=self._loads(row["state_json"]) or {},
            status=WorkflowStatus(row["status"]),
            consistency_mode=ConsistencyMode(row["consistency_mode"]),
            event_cursor=row["event_cursor"],
            result=self._loads(row["result_json"]),
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )

    @staticmethod
    def executor_name(task_id: str) -> str:
        # SkyPilot job names are also used in cloud resource names.  Keep the
        # readable prefix, but always append a digest so truncation/sanitizing
        # cannot make two logical task IDs adopt the same executor job.
        slug = re.sub(r"[^a-z0-9-]+", "-", task_id.lower()).strip("-")
        slug = slug or "task"
        digest = hashlib.sha256(task_id.encode()).hexdigest()[:10]
        return f"tpuswarm-{slug[:42]}-{digest}"

    def _insert_task(
        self,
        connection: sqlite3.Connection,
        spec: TaskSpec,
        *,
        workflow_id: str | None,
        component_key: str | None,
        generation: int | None,
        now: float,
    ) -> TaskRecord:
        try:
            connection.execute(
                """INSERT INTO tasks(
                       task_id, idempotency_key, kind, resource_class,
                       payload_json, metadata_json, status, priority,
                       recovery_priority, job_name, workflow_id, component_key,
                       generation, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    spec.task_id,
                    spec.idempotency_key,
                    spec.kind,
                    spec.resource_class,
                    self._dumps(spec.payload),
                    self._dumps(spec.metadata),
                    TaskStatus.PENDING.value,
                    int(spec.priority),
                    int(spec.recovery_priority),
                    self.executor_name(spec.task_id),
                    workflow_id,
                    component_key,
                    generation,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            if spec.idempotency_key is not None:
                row = connection.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?",
                    (spec.idempotency_key,),
                ).fetchone()
                if row is not None:
                    return self._task_from_row(row)
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (spec.task_id,)
            ).fetchone()
            if row is not None:
                return self._task_from_row(row)
            raise
        row = connection.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (spec.task_id,)
        ).fetchone()
        assert row is not None
        return self._task_from_row(row)

    def submit_task(self, spec: TaskSpec, *, now: float | None = None) -> TaskRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            return self._insert_task(
                connection,
                spec,
                workflow_id=None,
                component_key=None,
                generation=None,
                now=now,
            )

    def get_task(self, task_id: str) -> TaskRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return self._task_from_row(row)

    def list_tasks(self, *, status: TaskStatus | None = None) -> list[TaskRecord]:
        query = "SELECT * FROM tasks"
        params: tuple[Any, ...] = ()
        if status is not None:
            query += " WHERE status = ?"
            params = (status.value,)
        query += " ORDER BY created_at, task_id"
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._task_from_row(row) for row in rows]

    def set_resource_policy(
        self, policy: ResourcePolicy, *, now: float | None = None
    ) -> None:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            connection.execute(
                """INSERT INTO resource_policies(
                       resource_class, target_workers, recovery_reserve, updated_at
                   ) VALUES (?, ?, ?, ?)
                   ON CONFLICT(resource_class) DO UPDATE SET
                       target_workers = excluded.target_workers,
                       recovery_reserve = excluded.recovery_reserve,
                       updated_at = excluded.updated_at""",
                (
                    policy.resource_class,
                    policy.target_workers,
                    policy.recovery_reserve,
                    now,
                ),
            )

    def get_resource_policies(self) -> dict[str, ResourcePolicy]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM resource_policies").fetchall()
        return {
            row["resource_class"]: ResourcePolicy(
                row["resource_class"],
                row["target_workers"],
                row["recovery_reserve"],
            )
            for row in rows
        }

    def reserve_submissions(
        self, *, limit: int = 32, now: float | None = None
    ) -> list[TaskRecord]:
        """Atomically admits pending jobs without consuming recovery reserve."""

        now = time.time() if now is None else now
        if limit < 1:
            return []
        with self._transaction() as connection:
            policies = {
                row["resource_class"]: ResourcePolicy(
                    row["resource_class"],
                    row["target_workers"],
                    row["recovery_reserve"],
                )
                for row in connection.execute(
                    "SELECT * FROM resource_policies"
                ).fetchall()
            }
            active = {
                row["resource_class"]: row["count"]
                for row in connection.execute(
                    """SELECT resource_class, COUNT(*) AS count FROM tasks
                       WHERE status IN (?, ?, ?) GROUP BY resource_class""",
                    (
                        TaskStatus.SUBMITTING.value,
                        TaskStatus.SUBMITTED.value,
                        TaskStatus.RUNNING.value,
                    ),
                ).fetchall()
            }
            rows = connection.execute(
                """SELECT * FROM tasks WHERE status = ?
                   ORDER BY priority DESC, created_at, task_id""",
                (TaskStatus.PENDING.value,),
            ).fetchall()
            selected: list[TaskRecord] = []
            for row in rows:
                if len(selected) >= limit:
                    break
                task = self._task_from_row(row)
                policy = policies.get(task.resource_class)
                count = active.get(task.resource_class, 0)
                if policy is not None:
                    is_recovery = task.priority >= Priority.ORDINARY_RECOVERY
                    capacity = policy.target_workers
                    if not is_recovery:
                        capacity -= policy.recovery_reserve
                    if count >= capacity:
                        continue
                connection.execute(
                    """UPDATE tasks SET status = ?, submission_started_at = ?,
                           updated_at = ?, last_error = NULL WHERE task_id = ?""",
                    (TaskStatus.SUBMITTING.value, now, now, task.task_id),
                )
                active[task.resource_class] = count + 1
                updated = connection.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task.task_id,)
                ).fetchone()
                assert updated is not None
                selected.append(self._task_from_row(updated))
            return selected

    def bind_job(
        self,
        task_id: str,
        job_id: str,
        *,
        executor_status: str = "PENDING",
        now: float | None = None,
    ) -> TaskRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            cursor = connection.execute(
                """UPDATE tasks SET status = ?, job_id = ?, executor_status = ?,
                       updated_at = ? WHERE task_id = ? AND status IN (?, ?)""",
                (
                    TaskStatus.SUBMITTED.value,
                    str(job_id),
                    executor_status,
                    now,
                    task_id,
                    TaskStatus.SUBMITTING.value,
                    TaskStatus.SUBMITTED.value,
                ),
            )
            if cursor.rowcount != 1:
                raise TaskNotFoundError(f"task is not awaiting submission: {task_id}")
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            assert row is not None
            return self._task_from_row(row)

    def reset_submission(
        self, task_id: str, *, reason: str, now: float | None = None
    ) -> TaskRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            cursor = connection.execute(
                """UPDATE tasks SET status = ?, submission_started_at = NULL,
                       last_error = ?, updated_at = ?
                   WHERE task_id = ? AND status = ?""",
                (
                    TaskStatus.PENDING.value,
                    reason,
                    now,
                    task_id,
                    TaskStatus.SUBMITTING.value,
                ),
            )
            if cursor.rowcount != 1:
                raise TaskNotFoundError(f"task is not submitting: {task_id}")
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            assert row is not None
            return self._task_from_row(row)

    def fail_task(
        self, task_id: str, *, reason: str, now: float | None = None
    ) -> TaskRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFoundError(task_id)
            task = self._task_from_row(row)
            connection.execute(
                """UPDATE tasks SET status = ?, last_error = ?, updated_at = ?,
                       completed_at = ? WHERE task_id = ?""",
                (TaskStatus.FAILED.value, reason, now, now, task_id),
            )
            if task.status is not TaskStatus.FAILED:
                self._append_event(
                    connection,
                    task,
                    "TaskFailed",
                    {"error": reason},
                    now=now,
                )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            assert updated is not None
            return self._task_from_row(updated)

    def _append_event(
        self,
        connection: sqlite3.Connection,
        task: TaskRecord,
        kind: str,
        payload: Mapping[str, Any],
        *,
        now: float,
    ) -> None:
        if task.workflow_id is None:
            return
        connection.execute(
            """INSERT INTO workflow_events(
                   workflow_id, kind, component_key, generation,
                   payload_json, created_at
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                task.workflow_id,
                kind,
                task.component_key,
                task.generation,
                self._dumps(payload),
                now,
            ),
        )

    @staticmethod
    def _task_status(executor_status: str) -> TaskStatus:
        status = executor_status.upper()
        if status == "SUCCEEDED":
            return TaskStatus.SUCCEEDED
        if status.startswith("FAILED"):
            return TaskStatus.FAILED
        if status in {"CANCELLED", "CANCELLING"}:
            return TaskStatus.CANCELLED
        if status in {
            "RUNNING",
            "RECOVERING",
            "RECOVERING_INITIALIZING",
            "RECOVERING_RESOURCES",
        }:
            return TaskStatus.RUNNING
        return TaskStatus.SUBMITTED

    def observe_job(
        self, task_id: str, job: ExecutorJob, *, now: float | None = None
    ) -> TaskRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFoundError(task_id)
            task = self._task_from_row(row)
            new_status = self._task_status(job.status)
            completed_at = now if new_status.is_terminal else None
            result = task.result
            error = task.last_error
            if new_status is TaskStatus.SUCCEEDED:
                result = {
                    "job_id": job.job_id,
                    "recoveries": job.recovery_count,
                }
                error = None
            elif new_status is TaskStatus.FAILED:
                error = job.failure_reason or f"SkyPilot job ended as {job.status}"
            connection.execute(
                """UPDATE tasks SET status = ?, job_id = ?, executor_status = ?,
                       recovery_count = ?, result_json = ?, last_error = ?,
                       updated_at = ?, completed_at = ? WHERE task_id = ?""",
                (
                    new_status.value,
                    job.job_id,
                    job.status,
                    job.recovery_count,
                    None if result is None else self._dumps(result),
                    error,
                    now,
                    completed_at,
                    task_id,
                ),
            )
            if new_status != task.status:
                self._append_event(
                    connection,
                    task,
                    f"Task{new_status.value.title()}",
                    {
                        "job_id": job.job_id,
                        "executor_status": job.status,
                        "recovery_count": job.recovery_count,
                        "error": error,
                    },
                    now=now,
                )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            assert updated is not None
            return self._task_from_row(updated)

    def record_checkpoint(
        self,
        task_id: str,
        checkpoint: CheckpointRef,
        *,
        now: float | None = None,
    ) -> TaskRecord:
        """Publishes a monotonic reference after the object-store write commits."""

        now = time.time() if now is None else now
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFoundError(task_id)
            existing = connection.execute(
                "SELECT * FROM checkpoints WHERE task_id = ? AND sequence = ?",
                (task_id, checkpoint.sequence),
            ).fetchone()
            if existing is not None:
                if existing["uri"] != checkpoint.uri:
                    raise BarrierConflictError(
                        f"checkpoint {task_id}/{checkpoint.sequence} already has a different URI"
                    )
            else:
                connection.execute(
                    """INSERT INTO checkpoints(
                           task_id, sequence, uri, metadata_json, created_at
                       ) VALUES (?, ?, ?, ?, ?)""",
                    (
                        task_id,
                        checkpoint.sequence,
                        checkpoint.uri,
                        self._dumps(checkpoint.metadata),
                        now,
                    ),
                )
            current_sequence = row["checkpoint_sequence"]
            if current_sequence is None or checkpoint.sequence > current_sequence:
                connection.execute(
                    """UPDATE tasks SET checkpoint_uri = ?, checkpoint_sequence = ?,
                           checkpoint_metadata_json = ?, updated_at = ?
                       WHERE task_id = ?""",
                    (
                        checkpoint.uri,
                        checkpoint.sequence,
                        self._dumps(checkpoint.metadata),
                        now,
                        task_id,
                    ),
                )
            updated = connection.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            assert updated is not None
            return self._task_from_row(updated)

    def submit_workflow(
        self,
        spec: WorkflowSpec,
        components: Sequence[ComponentSpec],
        *,
        initial_state: Mapping[str, Any],
        now: float | None = None,
    ) -> WorkflowRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            connection.execute(
                """INSERT INTO workflows(
                       workflow_id, kind, payload_json, metadata_json, state_json,
                       status, consistency_mode, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    spec.workflow_id,
                    spec.kind,
                    self._dumps(spec.payload),
                    self._dumps(spec.metadata),
                    self._dumps(initial_state),
                    WorkflowStatus.ACTIVE.value,
                    spec.consistency_mode.value,
                    now,
                    now,
                ),
            )
            for component in components:
                task = self._insert_task(
                    connection,
                    component.task,
                    workflow_id=spec.workflow_id,
                    component_key=component.key,
                    generation=component.generation,
                    now=now,
                )
                connection.execute(
                    """INSERT INTO workflow_components(
                           workflow_id, component_key, generation, task_id,
                           required, created_at, updated_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        spec.workflow_id,
                        component.key,
                        component.generation,
                        task.task_id,
                        int(component.required),
                        now,
                        now,
                    ),
                )
            row = connection.execute(
                "SELECT * FROM workflows WHERE workflow_id = ?", (spec.workflow_id,)
            ).fetchone()
            assert row is not None
            return self._workflow_from_row(row)

    def get_workflow(self, workflow_id: str) -> WorkflowRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM workflows WHERE workflow_id = ?", (workflow_id,)
            ).fetchone()
        if row is None:
            raise WorkflowNotFoundError(workflow_id)
        return self._workflow_from_row(row)

    def list_workflows(
        self, *, status: WorkflowStatus | None = None
    ) -> list[WorkflowRecord]:
        query = "SELECT * FROM workflows"
        params: tuple[Any, ...] = ()
        if status is not None:
            query += " WHERE status = ?"
            params = (status.value,)
        query += " ORDER BY created_at, workflow_id"
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._workflow_from_row(row) for row in rows]

    def get_workflow_components(self, workflow_id: str) -> dict[str, TaskRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT c.component_key, t.* FROM workflow_components c
                   JOIN tasks t ON t.task_id = c.task_id
                   WHERE c.workflow_id = ? ORDER BY c.component_key""",
                (workflow_id,),
            ).fetchall()
        return {row["component_key"]: self._task_from_row(row) for row in rows}

    def replace_component(
        self,
        workflow_id: str,
        component: ComponentSpec,
        *,
        now: float | None = None,
    ) -> TaskRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            current = connection.execute(
                """SELECT generation FROM workflow_components
                   WHERE workflow_id = ? AND component_key = ?""",
                (workflow_id, component.key),
            ).fetchone()
            if current is not None and component.generation <= current["generation"]:
                row = connection.execute(
                    """SELECT t.* FROM workflow_components c JOIN tasks t
                       ON t.task_id = c.task_id
                       WHERE c.workflow_id = ? AND c.component_key = ?""",
                    (workflow_id, component.key),
                ).fetchone()
                assert row is not None
                return self._task_from_row(row)
            task = self._insert_task(
                connection,
                component.task,
                workflow_id=workflow_id,
                component_key=component.key,
                generation=component.generation,
                now=now,
            )
            connection.execute(
                """INSERT INTO workflow_components(
                       workflow_id, component_key, generation, task_id,
                       required, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(workflow_id, component_key) DO UPDATE SET
                       generation = excluded.generation,
                       task_id = excluded.task_id,
                       required = excluded.required,
                       updated_at = excluded.updated_at""",
                (
                    workflow_id,
                    component.key,
                    component.generation,
                    task.task_id,
                    int(component.required),
                    now,
                    now,
                ),
            )
            return task

    def get_workflow_events(
        self, workflow_id: str, *, after: int = 0
    ) -> list[WorkflowEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM workflow_events
                   WHERE workflow_id = ? AND event_id > ? ORDER BY event_id""",
                (workflow_id, after),
            ).fetchall()
        return [
            WorkflowEvent(
                event_id=row["event_id"],
                workflow_id=row["workflow_id"],
                kind=row["kind"],
                component_key=row["component_key"],
                generation=row["generation"],
                payload=self._loads(row["payload_json"]) or {},
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def save_workflow_state(
        self,
        workflow_id: str,
        state: Mapping[str, Any],
        event_cursor: int,
        *,
        now: float | None = None,
    ) -> WorkflowRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            connection.execute(
                """UPDATE workflows SET state_json = ?, event_cursor = ?,
                       updated_at = ? WHERE workflow_id = ?""",
                (self._dumps(state), event_cursor, now, workflow_id),
            )
            row = connection.execute(
                "SELECT * FROM workflows WHERE workflow_id = ?", (workflow_id,)
            ).fetchone()
            if row is None:
                raise WorkflowNotFoundError(workflow_id)
            return self._workflow_from_row(row)

    def complete_workflow(
        self,
        workflow_id: str,
        *,
        result: Mapping[str, Any],
        now: float | None = None,
    ) -> WorkflowRecord:
        return self._finish_workflow(
            workflow_id,
            WorkflowStatus.SUCCEEDED,
            result=result,
            error=None,
            now=now,
        )

    def fail_workflow(
        self, workflow_id: str, *, reason: str, now: float | None = None
    ) -> WorkflowRecord:
        return self._finish_workflow(
            workflow_id,
            WorkflowStatus.FAILED,
            result=None,
            error=reason,
            now=now,
        )

    def _finish_workflow(
        self,
        workflow_id: str,
        status: WorkflowStatus,
        *,
        result: Mapping[str, Any] | None,
        error: str | None,
        now: float | None,
    ) -> WorkflowRecord:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            connection.execute(
                """UPDATE workflows SET status = ?, result_json = ?,
                       last_error = ?, updated_at = ?, completed_at = ?
                   WHERE workflow_id = ?""",
                (
                    status.value,
                    None if result is None else self._dumps(result),
                    error,
                    now,
                    now,
                    workflow_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM workflows WHERE workflow_id = ?", (workflow_id,)
            ).fetchone()
            if row is None:
                raise WorkflowNotFoundError(workflow_id)
            return self._workflow_from_row(row)

    def commit_barrier(
        self,
        workflow_id: str,
        barrier: int,
        *,
        controller_checkpoint_uri: str | None = None,
        now: float | None = None,
    ) -> BarrierCommit:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            workflow = connection.execute(
                "SELECT * FROM workflows WHERE workflow_id = ?", (workflow_id,)
            ).fetchone()
            if workflow is None:
                raise WorkflowNotFoundError(workflow_id)
            rows = connection.execute(
                """SELECT c.component_key, c.required, t.checkpoint_uri,
                          t.checkpoint_sequence, t.checkpoint_metadata_json
                   FROM workflow_components c JOIN tasks t ON t.task_id = c.task_id
                   WHERE c.workflow_id = ?""",
                (workflow_id,),
            ).fetchall()
            missing = [
                row["component_key"]
                for row in rows
                if row["required"]
                and (
                    row["checkpoint_sequence"] != barrier
                    or row["checkpoint_uri"] is None
                )
            ]
            if missing:
                raise BarrierConflictError(
                    f"barrier {barrier} missing matching checkpoints for: "
                    + ", ".join(sorted(missing))
                )
            manifest = {
                row["component_key"]: {
                    "uri": row["checkpoint_uri"],
                    "sequence": row["checkpoint_sequence"],
                    "metadata": self._loads(row["checkpoint_metadata_json"]) or {},
                }
                for row in rows
                if row["required"]
            }
            encoded = self._dumps(manifest)
            existing = connection.execute(
                """SELECT * FROM workflow_commits
                   WHERE workflow_id = ? AND barrier = ?""",
                (workflow_id, barrier),
            ).fetchone()
            if existing is not None and (
                existing["manifest_json"] != encoded
                or existing["controller_checkpoint_uri"] != controller_checkpoint_uri
            ):
                raise BarrierConflictError(f"barrier {barrier} already committed")
            if existing is None:
                connection.execute(
                    """INSERT INTO workflow_commits(
                           workflow_id, barrier, controller_checkpoint_uri,
                           manifest_json, created_at
                       ) VALUES (?, ?, ?, ?, ?)""",
                    (
                        workflow_id,
                        barrier,
                        controller_checkpoint_uri,
                        encoded,
                        now,
                    ),
                )
                connection.execute(
                    """INSERT INTO workflow_events(
                           workflow_id, kind, payload_json, created_at
                       ) VALUES (?, ?, ?, ?)""",
                    (
                        workflow_id,
                        "BarrierCommitted",
                        self._dumps({"barrier": barrier}),
                        now,
                    ),
                )
            return BarrierCommit(
                workflow_id=workflow_id,
                barrier=barrier,
                controller_checkpoint_uri=controller_checkpoint_uri,
                components={
                    key: CheckpointRef(
                        uri=value["uri"],
                        sequence=value["sequence"],
                        metadata=value["metadata"],
                    )
                    for key, value in manifest.items()
                },
                created_at=existing["created_at"] if existing is not None else now,
            )

    def latest_barrier(self, workflow_id: str) -> BarrierCommit | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM workflow_commits WHERE workflow_id = ?
                   ORDER BY barrier DESC LIMIT 1""",
                (workflow_id,),
            ).fetchone()
        if row is None:
            return None
        manifest = json.loads(row["manifest_json"])
        return BarrierCommit(
            workflow_id=workflow_id,
            barrier=row["barrier"],
            controller_checkpoint_uri=row["controller_checkpoint_uri"],
            components={key: CheckpointRef(**value) for key, value in manifest.items()},
            created_at=row["created_at"],
        )

    def acquire_leadership(
        self,
        controller_id: str,
        *,
        lease_seconds: float,
        now: float | None = None,
    ) -> ControllerLease | None:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM controller_leadership WHERE singleton = 1"
            ).fetchone()
            if row is not None and (
                row["lease_expires_at"] > now and row["controller_id"] != controller_id
            ):
                return None
            token = 1 if row is None else row["fence_token"] + 1
            connection.execute(
                """INSERT INTO controller_leadership(
                       singleton, controller_id, fence_token, lease_expires_at,
                       updated_at
                   ) VALUES (1, ?, ?, ?, ?)
                   ON CONFLICT(singleton) DO UPDATE SET
                       controller_id = excluded.controller_id,
                       fence_token = excluded.fence_token,
                       lease_expires_at = excluded.lease_expires_at,
                       updated_at = excluded.updated_at""",
                (controller_id, token, now + lease_seconds, now),
            )
            return ControllerLease(controller_id, token, now + lease_seconds)

    def renew_leadership(
        self,
        lease: ControllerLease,
        *,
        lease_seconds: float,
        now: float | None = None,
    ) -> ControllerLease:
        now = time.time() if now is None else now
        with self._transaction() as connection:
            cursor = connection.execute(
                """UPDATE controller_leadership SET lease_expires_at = ?,
                       updated_at = ? WHERE singleton = 1 AND controller_id = ?
                       AND fence_token = ? AND lease_expires_at >= ?""",
                (
                    now + lease_seconds,
                    now,
                    lease.controller_id,
                    lease.fence_token,
                    now,
                ),
            )
            if cursor.rowcount != 1:
                raise LeadershipLostError(lease.controller_id)
        return ControllerLease(
            lease.controller_id, lease.fence_token, now + lease_seconds
        )
