# TPUSwarm

TPUSwarm is a small application-level scheduling layer over
[SkyPilot Managed Jobs](https://docs.skypilot.ai/en/latest/examples/managed-jobs.html).
It adds stable logical task IDs, idempotent submission, dynamic workflow state,
checkpoint barriers, and warm-capacity admission policy. It deliberately does
not implement its own remote worker, process lease, spot retry loop, regional
optimizer, log service, or infrastructure controller.

## Ownership boundary

SkyPilot owns:

- Managed Job process restart after spot preemption, node failure, or selected
  application exit codes.
- Regional failover (`EAGER_NEXT_REGION` / `FAILOVER`) and candidate resource
  selection (`any_of` / `ordered`).
- Reusable Job Pools, pending-job scheduling, and pool autoscaling.
- Multi-node jobs, sequential pipelines, and static parallel Job Groups.
- Workdir distribution, secrets, lifecycle hooks, GCS mounts, logs, status,
  cleanup, and API-server authentication/RBAC.

TPUSwarm owns only:

- `task_id` and `idempotency_key` across multiple submitter sessions.
- Trusted `AutoResumable` handlers that compile application payloads into
  native SkyPilot job definitions.
- Admission limits that intentionally keep part of a warm pool idle for a
  recovery or blocking child.
- `MultiAutoResumable` reducers for dynamic trees whose child set changes over
  time and cannot be represented by one static SkyPilot Job Group.
- Monotonic application checkpoint references and cross-component barriers.

For a static ensemble known at submission time, prefer a SkyPilot Job Group:
it already launches components in parallel and recovers a preempted component
without restarting its siblings. Use `MultiAutoResumable` only for dynamic
component generations, algorithmic barriers, or cross-region placement that a
single Job Group cannot express.

## Contracts

An `AutoResumable` does not run a second retry loop. It compiles a logical task
into `ManagedJobSpec`; the SkyPilot job's startup command must always probe for
completion and restore its latest durable checkpoint. Optional preemption
commands become native SkyPilot lifecycle hooks and remain best effort.

A `MultiAutoResumable` is a durable event reducer. It declares desired child
keys and generations. Each child is a normal SkyPilot Managed Job, so native
recovery remains local to that component. A terminal child event is presented
to the reducer only after SkyPilot has exhausted the job's configured recovery.

## Run the thin controller

Deploy a remote SkyPilot API server first. The TPUSwarm process only stores
logical metadata and talks to that server with the normal SkyPilot SDK:

```bash
export TPUSWARM_TOKEN="$(openssl rand -hex 32)"
export SKY_API_SERVER_URL=https://your-skypilot-api.example.com

uv run --isolated --extra server --extra skypilot tpuswarm serve \
  --database /var/lib/tpuswarm/tpuswarm.db
```

The initial store is SQLite for one externally supervised controller. Put it on
a durable CPU disk and use [`examples/tpuswarm.service`](examples/tpuswarm.service).
A production multi-replica deployment should replace it with PostgreSQL; the
SkyPilot API server already supports its own resilient deployment and database.

Register project handlers on the server with repeatable modules:

```bash
tpuswarm serve ... --registry-module my_project.tpuswarm_handlers
```

Each module exports `register(registry)`.

## Warm TPU pool

Create the pool once:

```bash
sky jobs pool apply --pool tpuswarm-v6e8 examples/v6e8-pool.yaml -y
```

TPUSwarm submits actual training jobs to this pool; there is no permanent
consumer job occupying every worker. Configure nine workers with one recovery
reserve:

```bash
curl -fsS -X PUT "$TPUSWARM_SERVER/v1/resources/gcp-tpu-v6e-8" \
  -H "Authorization: Bearer $TPUSWARM_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"target_workers": 9, "recovery_reserve": 1}'
```

At most eight ordinary jobs are admitted. The ninth worker stays ready for a
high-priority recovery/dynamic child. SkyPilot itself keeps the worker target,
moves a recovering Managed Job through its queue, and restores the process from
the workload's durable checkpoint.

## Submit

```bash
curl -fsS -X POST "$TPUSWARM_SERVER/v1/tasks" \
  -H "Authorization: Bearer $TPUSWARM_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @examples/command-task.json
```

The built-in command handler supports a completion probe, preemption hook,
native `resources.job_recovery`, a pool, environment variables, secrets, and
the remaining standard SkyPilot task fields. Pool jobs may not specify setup,
workdir, or file mounts; those belong in the pool definition.
Secret values may be `null`; TPUSwarm resolves those names from the controller
environment immediately before launch and does not persist the resolved value.

For checkpoints, prefer a SkyPilot GCS mount with `MOUNT_CACHED` and
`MODEL_CHECKPOINT_RW`, or the application's existing atomic GCS uploader. The
payload must be durable before publishing its URI to TPUSwarm. A successful
SkyPilot job already waits for cached mount writes to flush before completion.

## Controller recovery

After restart, TPUSwarm reloads logical state, queries SkyPilot's persisted jobs,
and adopts them by managed job ID or the stable name derived from `task_id`.
SkyPilot continues running/recovering those jobs while TPUSwarm is unavailable.
A submission grace window avoids duplicating a job if the local process crashes
between the remote launch request and recording its returned job ID.

For a controller that is itself a SkyPilot Managed Job, use a durable database
and leave `api_server_access: true`; SkyPilot injects scoped API credentials so
the recovered controller can reconnect and launch child jobs. Do not place the
SkyPilot API server inside the TPUSwarm workflow it is responsible for running.
