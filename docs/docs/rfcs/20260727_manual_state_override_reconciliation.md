- Feature Name: Reconciling manually-overridden Airflow run states
- Status: draft
- Start Date: 2026-07-27
- Authors: muhammad.fahlevi

# Summary

**What** — Detect when an engineer manually changes a task/DAG state in Airflow and bring
Optimus's `job_run` / `task_run` / `sensor_run` / `hook_run` rows back in line.

**Why** — Manual overrides bypass the operator callbacks Optimus relies on, so no event is posted.
Optimus keeps a stale `running`/`failed`, producing false-positive incompleteness alerts.

**How** — A background worker polls each Airflow's `log` (audit) table for human-driven state
changes, maps them onto Optimus's run tables, and reconciles.

**Impact** — New worker, new config, one new table, additive repository methods. No API or
DAG-template change. Writes land in tables that feed alerting, SLA prediction and duration
estimation, so write scope must be conservative.

# Verified behaviour of Airflow 2.9.3

Confirmed against production `log` rows and by reading the 2.9.3 source in the running image.
Airflow app version 2.9.3 (chart 1.15.0, image `2.9.3-python3.12-1.1.0`).

### V1. The audit-log approach works; the bare event names are the right filter

The 2.9 grid UI posts to the **legacy `www` form endpoints**, so human marks appear as bare
`success` / `failed` / `dagrun_success` / `dagrun_failed`. Their `extra` is the tell:

```
success        | extra = {"confirmed":"true","past":"false","future":"false",
                          "upstream":"false","downstream":"false"}
dagrun_success | extra = {"confirmed":"true"}
```

Requests served under `/api/v1` are instead prefixed `api.` / `ui.`
(`api.patch_task_instance`, `api.post_set_task_instances_state`, `api.update_dag_run_state`,
`api.post_clear_task_instances`, `api.clear_dag_run`).

**Useful consequence:** Optimus's own replay/clear calls go through `/api/v1`, so they land under
`api.*` and are naturally excluded by a bare-name filter. The feedback-loop risk of the reconciler
fighting the replay worker is therefore already mitigated — provided the filter stays on bare
names. Worth an explicit test so a future refactor doesn't silently widen it.

**Residual gap:** state changes driven through the REST API by anything other than Optimus
(scripts, other tooling) produce `api.*` events and will be missed. Decide whether that is in
scope; if so, those need separate handling because their shape differs (see V4).

### V2. Worker transitions also write `success`/`failed`, but are distinguishable

`airflow/models/taskinstance.py` and `dagrun.py` insert `Log` rows for ordinary transitions:

```
 id   | event   | task_id | owner             | owner_display_name | extra
 1410 | success | python  | @muhammad.fahlevi | NULL               | NULL
```

`owner` is the **DAG owner**, `owner_display_name` is NULL, `extra` is NULL. So
`owner_display_name IS NOT NULL` correctly isolates human actions, as proposed.

Two caveats:
- `self.owner_display_name = owner_display_name or None` — an **anonymous** UI action becomes
  NULL too. Not an issue with SSO enforced (production shows `google_…` owners with real display
  names), but it is why `extra IS NOT NULL` is a slightly stronger equivalent discriminator.
- `GET /api/v1/eventLogs` **does not expose `owner_display_name`** (nor `map_index`). The
  discriminator is therefore only available via **direct DB access** — see V6.

### V3. `confirmed=false` rows are previews that changed nothing

`_mark_dagrun_state_as_success/failed` call `set_dag_run_state_to_*(…, commit=confirmed)`, and
`@action_logging` writes its row **before** the handler runs. So a `dagrun_success` row with
`extra->>'confirmed' = 'false'` logged an action that **did not happen**.

`success`/`failed` (task-level) ignore `confirmed` in 2.9.3 and always apply.

**Required filter addition:** `AND (event NOT LIKE 'dagrun_%' OR extra::jsonb->>'confirmed' = 'true')`,
or simply require `confirmed = 'true'` for all four events.

More generally, because the row is written first, 404s and permission-denied attempts also log.

### V4. Bulk flags change many task instances but write one row

`extra` carries `past`/`future`/`upstream`/`downstream`. When any is `"true"`, a single log row
corresponds to an unknown set of changed task instances — potentially across **other DAG runs**
(`past`/`future` span execution dates).

The audit log cannot enumerate them. Options: (a) treat any bulk-flagged row as a trigger to
re-read actual state for that DAG from Airflow, or (b) explicitly scope v1 to
`past=future=upstream=downstream=false` and count the rest in a metric. Option (b) is a
defensible v1 as long as the gap is measured rather than silent.

### V5. Airflow's own cascade is asymmetric — mirror it, don't force-set

From `airflow/api/common/mark_tasks.py`:

- `set_dag_run_state_to_failed`: sets the dag run failed, then marks **only**
  `RUNNING` / `DEFERRED` / `UP_FOR_RESCHEDULE` task instances failed, marks other unfinished ones
  `SKIPPED`, and **leaves already-terminal tasks untouched**.
- `set_dag_run_state_to_success`: sets all task instances to success.

So the proposed "cascade update to force all associated children into the target state" is wrong
for the failure case — it would rewrite genuinely-successful task rows to `failed`, corrupting the
duration/percentile history that `GetPercentileDurationByJobNames` and the SLA predictor consume.

Note Airflow's `SKIPPED` has **no Optimus equivalent** (`core/scheduler/status.go` has no
`skipped`), so those instances need an explicit decision — most likely leave untouched.

### V6. REST vs direct DB

Because the discriminator (`owner_display_name`) is not exposed over REST, and because there is no
`id >` predicate on `eventLogs` (only `after`/`before` on `dttm`), the original instinct to read
the `log` table directly is justified. It is nevertheless a **new dependency**: Optimus currently
has exactly one DSN (`serve.db.dsn`) and has never connected to Airflow's metadata DB.

That brings: a second pool, a read-only Airflow DB user + secret, a network path, and coupling to
Airflow's internal schema across upgrades. All acceptable, but they should be a conscious,
documented decision rather than a side effect — and the read-only grant matters, since this is a
write-capable database that Optimus has no business writing to.

### V7. There is no single Airflow

`SCHEDULER_HOST` is **project-level** config (`ext/scheduler/airflow/airflow.go:626`), so there are
N Airflow instances with independent `log.id` sequences. A single global `last_processed_log_id`
is incorrect — the watermark must be keyed per scheduler host, as must the DB connection.

# Technical design

## Point 5 — synchronisation between replicas (revised)

**Do not use `pg_try_advisory_lock`:**

1. The codebase contains no advisory locks anywhere; the established idiom is a lease claim.
2. Session-level advisory locks bind to a specific pooled connection. With `pgxpool`,
   `pg_advisory_unlock` can run on a *different* connection, silently leaking the lock until that
   connection is recycled.
3. `pg_advisory_xact_lock` avoids the leak but forces the cycle into one long transaction, holding
   a connection and generating bloat.

**Use the existing lease pattern** — mirror `GetExpiredSLAsForProcessing`
(`internal/store/postgres/scheduler/sla_repository.go:124`). Co-locate lease and watermark in one
row keyed per scheduler host (V7), so claim + watermark read are atomic:

```sql
UPDATE airflow_sync_state
   SET worker_signature = $1, worker_lock_until = now() + $2, updated_at = now()
 WHERE scheduler_host = $3
   AND (worker_lock_until IS NULL OR worker_lock_until < now())
RETURNING last_processed_log_id, last_processed_dttm;
```

Gate on `RowsAffected() == 1`; losers skip the cycle. Crash recovery is automatic via expiry.

**Fence the write-back** — a slow worker whose lease expired must not clobber a newer watermark:

```sql
UPDATE airflow_sync_state
   SET last_processed_log_id = $1, last_processed_dttm = $2, updated_at = now()
 WHERE scheduler_host = $3
   AND worker_signature = $4
   AND worker_lock_until > now();
```

0 rows affected ⇒ lease lost mid-cycle ⇒ discard this cycle's progress and log it.

Bound each cycle's batch so it cannot outlive the lease, and set `worker_lock_until` above p99
cycle time. Note `server/optimus.go:247` cancels worker contexts then immediately closes the DB
pool without waiting, so check `ctx.Err()` between steps or every shutdown logs spurious errors.

## Point 6 — codebase changes (revised)

| File | Change |
|---|---|
| `migrations/000NNN_create_airflow_sync_state.{up,down}.sql` | `scheduler_host TEXT PRIMARY KEY`, `last_processed_log_id BIGINT`, `last_processed_dttm TIMESTAMPTZ`, `worker_signature UUID`, `worker_lock_until TIMESTAMPTZ`, timestamps |
| `internal/store/postgres/scheduler/airflow_sync_state_repository.go` | new: `ClaimForProcessing`, `CommitWatermark`, both fenced |
| `ext/scheduler/airflow/audit_log_reader.go` (new, in `package airflow`) | the Airflow-DB read. Keep it behind an interface so the service is testable and a future REST implementation can swap in |
| `internal/store/postgres/scheduler/job_run_repository.go` | additive: lookup by `(project, job_name, scheduled_at)` in batch; batch state update |
| `internal/store/postgres/scheduler/job_operator_repository.go` | additive: fetch/update newest child by `(job_run_id, name)`. Keep the `operatorTypeToTableName` seam |
| `core/scheduler/service/airflow_state_sync_service.go` | new: parse rows → resolve entity → apply write-scope rules |
| `core/scheduler/service/airflow_state_sync_worker.go` | new: ticker + lease + per-host fan-out, modelled on `sla_worker.go:54` |
| `core/scheduler/job_run.go` | shared domain types (`scheduler` cannot import `service`) |
| `core/scheduler/status.go` | Airflow→Optimus state map + ignore-list (see below) |
| `config/config_server.go` | `AirflowSyncConfig`: interval, lock duration, per-host DSNs, lookback cap, batch size. `mapstructure` + `default:` tags |
| `server/optimus.go` | wire next to the SLA worker (~line 479), guarded by `interval > 0` |
| `config.sample.yaml`, `dev/optimus.values.yaml` | document new keys |

Deviations from the original: a dedicated repository for lease+watermark rather than piling onto
`job_run_repository.go`, and the Airflow-side read isolated behind its own interface rather than
extending `client.go` (which is REST-shaped — `Client.Invoke` takes the unexported
`airflowRequest`, so a DB reader does not belong there).

## Entity resolution

1. `dag_id` → Optimus job. On a shared Airflow, `dag_id` is only unique per project — resolve via
   the project owning that `SCHEDULER_HOST`, and no-op on unknown DAGs.
2. `run_id` → `scheduled_at`. `execution_date` is **NULL** in these rows; only `run_id` is
   populated, and it comes in at least three shapes seen in production:
   - `scheduled__2026-07-24T18:00:00+00:00`
   - `custom-backfill_<uuid>__2026-07-25T09:38:53+00:00`
   - `replayed__2025-05-29T18:00:00+00:00`
   Parse the trailing timestamp, then apply the **same shift `__lib.py` uses**:
   `croniter(interval, execution_date).get_next()`. Optimus's `scheduled_at` is *not*
   `execution_date`; skipping this puts every row one interval off.
3. `task_id` → run type via prefix: `wait_*` → `sensor_run`, `hook_*` → `hook_run`, otherwise
   `task_run`. **This classifier does not exist in Go today** — it lives only in
   `__lib.py get_run_type`. Add it next to `core/scheduler/job.go:48` and reuse `OperatorType`,
   ideally sharing the prefixes with the DAG template rather than re-hardcoding them.
4. Child rows have no unique constraint and retries insert duplicates; update the newest by
   `created_at`, matching `GetOperatorRun`'s `ORDER BY created_at DESC LIMIT 1`.

## Write-scope rules

- **Only map to states `StateFromString` accepts.** `canceled`, `up_for_retry`, `restarting`,
  `missing` are rejected on read (`core/scheduler/status.go:36`); persisting them makes later reads
  fail hard. Airflow's `skipped`/`upstream_failed`/`removed`/`deferred` have no equivalent — use an
  explicit map with an ignore-list, never a naive string pass-through.
- **Mirror Airflow's asymmetric cascade (V5).** On `dagrun_failed`, only update non-terminal
  children; never rewrite an existing `success`.
- **Never regress fresher data.** A callback may have landed after the manual action. Compare the
  log row's `dttm` against the row's `updated_at` and skip if ours is newer.
- **Do not invent `end_time`.** Writing `now()` skews duration estimation for every future
  prediction on that job. Prefer the log `dttm`, or leave it alone.
- **Prefer not to roll up `job_run` from siblings.** Marking one task success does not finish the
  DAG; Airflow recomputes the dag-run state and the DAG-level callback normally still fires, so
  `job_run` often self-heals. Rolling up risks declaring a job complete while downstream tasks are
  still running. (`IsAllTerminated` / `IsAnyFailure` in `status.go` remain available if wanted.)
- **Emit state-change events.** Call `raiseJobRunStateChangeEvent`
  (`core/scheduler/service/job_run_service.go:960`) so downstream consumers don't desync — but
  decide deliberately whether it should alert (see unresolved question 1).

## Edge cases

**Correctness**
1. **Sequence gap.** Transaction A takes `id` 100, B takes 101 and commits first. A read at that
   instant sees 101, advances the watermark past 100, and 100 is skipped forever. Mitigate with a
   settling delay — only process rows with `dttm < now() - 60s`. The same delay avoids racing
   in-flight callbacks.
2. Ordering — apply rows in ascending `id`, and make writes idempotent so a replayed cycle is a
   no-op. Production data shows genuinely messy sequences (a `dagrun_success`, then a task
   `success`, then another `dagrun_success` 20s apart on the same run).
3. Clock skew between Optimus and Airflow if `dttm` is used for resume; keep a safety overlap and
   de-duplicate on `id`.
4. `airflow db clean` truncates `log`; the watermark must tolerate its rows vanishing.
5. `map_index` (dynamic task mapping) is not modelled by Optimus's child tables — ignore
   explicitly rather than by accident.

**Scope / mapping**
6. `custom-backfill_*` and `replayed__*` runs both appear in the data, and a human marking a
   *replayed* run success is a real observed case — decide whether these are in scope.
7. Renamed/deleted jobs and non-Optimus DAGs on a shared Airflow must no-op quietly.
8. Task-group marks (`group_id` instead of `task_id`) hit the same endpoints and log `task_id`
   NULL — detect and skip, or expand.

**Operational**
9. One unreachable Airflow DB must not starve other projects — isolate per host and still advance
   the others.
10. First run against a long backlog could be enormous — cap the initial lookback and batch size.
11. Skip projects with `DISABLE_JOB_SCHEDULING`.
12. Emit `airflow_state_reconciled_total{reason,result}` so divergence stays visible. A reconciler
    silently repairing a broken callback path is a monitoring blind spot.

# Drawbacks

- New coupling to Airflow's internal `log` schema and to view-function names, neither of which is
  a public API. Both can change on upgrade. Mitigation: keep the event names and `extra` keys in
  one well-commented constant block, and add a test that fails loudly if the shape changes.
- A second DB dependency (V6).
- Bulk-flagged rows are not fully handled in v1 (V4).

# Rationale and Alternatives

- **State reconciliation** (poll Airflow for the current state of runs Optimus thinks are
  unfinished, and diff) — immune to event-name coupling, bulk-op blindness and the
  attempt-vs-outcome gap, needs no Airflow DB access, and additionally repairs state lost to
  dropped callbacks or webserver 5xx. Costs more requests and loses attribution. Worth keeping in
  mind as the v2 if audit-log parsing proves brittle across upgrades; the two are compatible
  (audit log for attribution, reconciliation for correctness).
- **REST `eventLogs` polling** — no new DB dependency, but cannot express the
  `owner_display_name` discriminator or an `id >` predicate (V2, V6).
- **Airflow listener plugin** — the correct long-term fix, but requires Airflow 3.x.
- **Push from `__lib.py`** — cannot help; manual overrides are exactly the case where no task code
  runs.

# Unresolved questions

1. **Should reconciliation alert?** Suppress on `failed`→`success`; what about a late
   `running`→`failed`? Product decision.
2. Are `clear` operations in scope? A cleared task re-runs and self-heals via callbacks, but a
   cleared-then-never-scheduled task leaves a stale row.
3. Bulk-flagged rows (V4): scope out with a metric, or trigger a state re-read?
4. Backfill (`custom-backfill_*`) and replay (`replayed__*`) runs in scope?
5. REST-driven (`api.*`) overrides from non-Optimus tooling in scope?
6. Poll interval and lookback defaults (10 min proposed; interacts with the settling delay in
   edge case 1).
7. How are per-host Airflow DSNs supplied and rotated — server config, or a per-project secret
   alongside `SCHEDULER_AUTH`?
