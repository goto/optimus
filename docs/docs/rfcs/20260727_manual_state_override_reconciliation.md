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

### V6. REST is viable — `extra` is exposed, so `owner_display_name` is not needed

**Decision: fetch via `GET /api/v1/eventLogs`.** No Airflow-DB dependency.

`owner_display_name` is not exposed over REST, but it turns out not to be needed: **`extra` is
exposed**, and `extra` alone separates human from worker rows more precisely than
`owner_display_name` does. See "Identifying manual actions" below.

Mechanics of the endpoint, read from `api_connexion/endpoints/event_log_endpoint.py`:

- Filters: `dag_id` (single value only — `Log.dag_id == dag_id`, no list), `task_id`, `run_id`,
  `owner`, `event`, `included_events` / `excluded_events` (comma-separated), `before`, `after`.
- **Both time bounds are strict**: `Log.dttm < before` and `Log.dttm > after`.
- `order_by` accepts `event_log_id` (→ `id`) and `when` (→ `dttm`).
- `limit` is capped by the `[api] maximum_page_limit` config (default 100), so **pagination via
  `offset` is mandatory**; `total_entries` is returned so truncation is detectable.
- `extra` comes back as a **JSON-encoded string**, not a nested object
  (`"extra": "{\"confirmed\": \"true\"}"`) — Go must unmarshal the string, then its contents.

Two consequences that shape the design:

1. **Strict-on-both-ends bounds create a boundary hole.** With windows `(t0, t1)` then `(t1, t2)`,
   a row whose `dttm` is exactly `t1` is excluded from *both* (`< t1` fails, `> t1` fails).
   Microsecond precision makes it unlikely, but "unlikely" is not a correctness argument for a
   reconciliation feature. Overlap each window by a small epsilon and de-duplicate on
   `event_log_id`.
2. **Fixed closed windows make pagination safe.** Because the upper bound is fixed, rows arriving
   mid-pagination land beyond `before` and cannot shift pages. An open-ended `after`-only watermark
   would not have this property. This is a real advantage of the windowing approach.

### V7. There is no single Airflow

`SCHEDULER_HOST` is **project-level** config (`ext/scheduler/airflow/airflow.go:626`), so there are
N Airflow instances with independent `log.id` sequences. A single global `last_processed_log_id`
is incorrect — the watermark must be keyed per scheduler host, as must the DB connection.

# Technical design

## Identifying manual actions

**Gate on `extra`; record `owner` but do not gate on it.**

The precise question is "was this row written by `@action_logging`?", because that decorator only
runs on webserver request handlers — never on the scheduler/worker path. Its REST-visible
fingerprint is `extra`:

| Source | `extra` | `owner` |
|---|---|---|
| worker / scheduler transition | `NULL` | DAG owner (`@someone`) |
| human, task mark (`success`/`failed`) | `{"confirmed","past","future","upstream","downstream"}` | auth username (`google_…`) |
| human, dagrun mark (`dagrun_*`) | `{"confirmed"}` | auth username |

### Why `extra IS NOT NULL` is provably sufficient

There are exactly **seven** places in Airflow 2.9.3 that write a `log` row (verified by grepping
the installed package):

| Writer | event | `extra` |
|---|---|---|
| `taskinstance.py:2310` | `running` | NULL |
| `taskinstance.py:2498` | `self.state` (`success`/`failed`/…) | NULL |
| `taskinstance.py:2565` | `self.state` | NULL |
| `taskinstance.py:2920` | `failed` | NULL |
| `dagrun.py:598` | `paused` | populated, `owner_display_name='Scheduler'` |
| `utils/cli.py` (`action_cli`) | `cli_<sub_command>` | populated |
| `www/decorators.py:137` (`action_logging`) | view function name | **always** populated |

Restricted to the four target events, the only writers that can produce them are the
`taskinstance.py` ones (always `extra = NULL`) and `action_logging` (always `extra` populated).
`dagrun_success` / `dagrun_failed` have no model-code writer at all — only `action_logging`.

So **within the four-event filter, `extra IS NOT NULL` ⟺ the row came from `action_logging`.**
That is a structural argument, not an empirical one.

Note `dagrun.py:598` also disproves the general form of the original heuristic: a **scheduler**-written
row carries `owner_display_name = 'Scheduler'`. It is harmless here because `event='paused'` is
filtered out, but it means `owner_display_name IS NOT NULL` should not be the primary signal.

### Empirical check against production (2026-07-27)

```sql
select * from log where event in ('success','dagrun_success')
  and owner_display_name is not null
  and (owner not like 'google_%' or (extra is null or extra::jsonb->>'confirmed' is null))
  and dttm > '2026-07-01T00:00:00Z' limit 1;
```

Returned **no rows**. So for every row in this deployment since 2026-07-01 that the
`owner_display_name IS NOT NULL` filter classifies as manual, `owner LIKE 'google_%'` **and**
`extra->>'confirmed'` is present. Both candidate discriminators hold in practice.

Follow-ups run on 2026-07-27, both clean:

- The inverse query (`extra IS NOT NULL AND owner_display_name IS NULL`) returned **0 rows** — no
  anonymous-user blind spot in this deployment.
- `confirmed` is never `'false'`: `dagrun_success` 594 / `dagrun_failed` 32, all `'true'`, since
  2026-07-01. So the preview case (V3) does not occur in practice here — the guard stays as
  cheap insurance, not an active fix.

The remaining unverified assumption is the `owner LIKE 'google_%'` pattern itself. The decisive
test is **ungated** (unlike the query above, it does not pre-filter on `owner_display_name`), and
works because `extra IS NOT NULL` on these four events already proves `action_logging` wrote the
row — so every owner it lists is definitionally a manual actor:

```sql
select owner, count(*) from log
where event in ('success','failed','dagrun_success','dagrun_failed')
  and extra is not null
  and dttm > '2026-07-01T00:00:00Z'
group by 1 order by 2 desc;
```

If every owner returned starts with `google_`, the pattern is safe for this deployment. Anything
else in that list is precisely a manual override an `owner LIKE 'google_%'` gate would drop.

Two things the original query cannot establish, by construction:

1. **Selection effect.** It samples only rows already matching `owner_display_name IS NOT NULL`, so
   it cannot detect a manual mark that has `owner_display_name` NULL and is therefore missing from
   the sample entirely (the anonymous-user case). The direct test is the inverse query — since
   `extra IS NOT NULL` on these events *proves* `action_logging` wrote the row, any hit is a manual
   action the `owner_display_name` filter would silently drop:

   ```sql
   select id, dttm, event, dag_id, owner, owner_display_name, extra from log
   where event in ('success','failed','dagrun_success','dagrun_failed')
     and extra is not null and owner_display_name is null
     and dttm > '2026-07-01T00:00:00Z' limit 5;
   ```

2. **Whether `confirmed` is ever `'false'`.** The query tests presence (`IS NULL`), not value. For
   `dagrun_*` a `confirmed='false'` row is a preview that changed nothing (V3), so this has a live
   correctness impact:

   ```sql
   select event, extra::jsonb->>'confirmed' as confirmed, count(*) from log
   where event in ('dagrun_success','dagrun_failed')
     and dttm > '2026-07-01T00:00:00Z' group by 1,2 order by 1,2;
   ```

### `confirmed` is load-bearing for `dagrun_*` only

`confirmed` must **not** be required for the task-level events:

- `_mark_dagrun_state_as_success/failed` pass `commit=confirmed`, so for `dagrun_*` a
  `confirmed=false` row is a preview that changed nothing. Requiring `"true"` there is both
  correct and necessary (V3).
- `Airflow.success()` / `.failed()` **ignore `confirmed` entirely** in 2.9.3 —
  `_mark_task_instance_state` has no such parameter. The mark applies whether or not the client
  sends it. `extra` only contains what the client actually sent, so requiring `confirmed` on
  task-level events would **silently drop a real override** from any client that omits it. The grid
  UI does send it, but it is not load-bearing, which makes it a fragile gate.

Final filter:

```
event ∈ {success, failed, dagrun_success, dagrun_failed}   -- pushed down via included_events
AND extra parses as a non-null JSON object                 -- the human/worker discriminator
AND (event NOT LIKE 'dagrun_%' OR extra["confirmed"] == "true")   -- drop no-op previews
```

### One honest limitation

This identifies "arrived through the legacy `www` mark endpoint", not strictly "a human clicked in
the UI". A script doing a form POST to `/success` produces a byte-identical row. That is acceptable
— arguably desirable, since it is still an external override Optimus must reconcile — but the
wording of any alert should say "manually marked", not "marked by a user in the UI".

**Why not gate on `owner LIKE 'google_%'`:**

- It hardcodes one SSO provider into an open-source project. Any deployment on LDAP, local FAB
  users, or a different OAuth provider silently stops detecting overrides.
- It fails **silently** — the feature reports "no manual changes found", which is
  indistinguishable from success. For a correctness feature that is the worst failure mode.
- ANDing it with the `extra` check doubles the number of ways detection can silently break, while
  adding no discriminating power the `extra` check doesn't already have.

**What `owner` is genuinely good for:**

- **Attribution** — carry it through to the reconciliation log line, metric label (bounded
  cardinality permitting) and any alert body. "Marked success by X at T" is exactly the context an
  on-call needs. This is worth doing.
- **Exclusions** — an optional configurable deny-list, if a service account ever starts driving
  the legacy endpoints and should be ignored. An exclusion that fails open is safe; an inclusion
  pattern that fails closed is not.

**Upgrade tripwire.** Since `extra`'s shape is a UI form contract, not an API, validate it rather
than trusting it: for `success`/`failed` expect the five keys
(`confirmed`/`past`/`future`/`upstream`/`downstream`), for `dagrun_*` expect `confirmed`. On an
unexpected shape, **process the row anyway** (fail open — `extra IS NOT NULL` already proved it is
a manual action) but emit a warning + metric. That converts an Airflow-upgrade regression into an
alert rather than silent data loss. Failing open matters here: the four-event filter is the
correctness gate, the shape check is only a canary.

## Windowing and watermark

Confirmed design: `airflow_sync_state` stores explicit `(start_time, end_time)` windows; the next
window is `(previous_end_time, previous_end_time + interval)`.

This is a good choice — it makes gaps impossible by construction, windows explicit and re-runnable,
and pagination stable (V6). Four things it needs to be correct:

1. **Clamp the upper bound.** Only claim a window when
   `previous_end_time + interval <= now() - settling_delay` (≈60s). Processing a window whose end
   is too recent, then advancing past it, permanently misses rows whose transaction had not yet
   committed when queried. This is the REST equivalent of the sequence-gap hazard.
2. **Overlap by epsilon + de-duplicate on `event_log_id`,** because both bounds are strict (V6).
   Query `after = start_time - ε`, and keep the highest processed `event_log_id` per window (or a
   short-lived seen-set) to drop the re-delivered rows.
3. **Catch-up loop.** After downtime, one window per tick falls permanently behind. Process
   windows in a bounded loop per tick until caught up, and cap the very first window's lookback so
   a fresh deployment doesn't try to ingest all of history. **Revised:** the lookback cap is not a
   separate config value — it's derived as `WindowInterval × MaxWindowsPerTick`, so a fresh
   project's entire backlog always fits inside one tick's catch-up budget by construction, rather
   than risking a lookback that's set larger than what the per-tick loop can actually drain.
4. **Crash recovery** — see the table design below.

## `airflow_sync_state` design

```sql
CREATE TABLE IF NOT EXISTS airflow_sync_state (
    id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_name         VARCHAR(100) NOT NULL,
    start_time           TIMESTAMPTZ  NOT NULL,
    end_time             TIMESTAMPTZ  NOT NULL,

    status               VARCHAR(30)  NOT NULL,
    attempt_count        INT          NOT NULL DEFAULT 0,
    last_error           TEXT,

    worker_id            UUID,
    locked_until         TIMESTAMPTZ,

    -- highest event_log_id applied in this window. The API *does* return this as
    -- `event_log_id` (see the sample response above), so it is available over REST.
    -- Needed to de-duplicate the epsilon overlap between adjacent windows.
    max_processed_log_id BIGINT,

    -- observability only, not required for correctness:
    --   events_matched  = rows in this window that passed the manual-override filter
    --   runs_reconciled = Optimus run rows actually updated as a result
    -- The *gap* between them is the useful signal: matched > 0 with reconciled = 0
    -- means resolution is broken (wrong croniter shift, unknown dag_id, …) — a failure
    -- that is otherwise completely silent.
    events_matched       INT,
    runs_reconciled      INT,

    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uniq_airflow_sync_window UNIQUE (project_name, start_time, end_time)
);
CREATE INDEX IF NOT EXISTS airflow_sync_state_project_end_time_idx
    ON airflow_sync_state (project_name, end_time DESC);
```

Types follow existing precedent (`job_run`: `VARCHAR(100)` project name, `VARCHAR(30)` status,
`uuid_generate_v4()` PK, plain varchar status rather than a PG enum).

### Why a composite UNIQUE rather than a concatenated `sync_id`

A string key of the form `<project>_<start>_<end>` works, but makes uniqueness depend on **every
pod formatting the timestamp identically**. That is a live hazard here: this deployment's psql
renders `dttm` as `+07` while the API returns `+00:00`, so `…T10:00:00Z` and `…T17:00:00+07:00`
denote the same instant but are different strings — and therefore different `sync_id`s, which would
let two pods each "own" the same window.

`UNIQUE (project_name, start_time, end_time)` on `TIMESTAMPTZ` columns removes the class of bug
entirely, because Postgres compares instants, not text. Keep a derived `sync_id` for log lines if
useful, but do not let it carry uniqueness.

### Status values

Three terminal-or-not states, not four:

| status | meaning |
|---|---|
| `in_progress` | claimed by a pod; `worker_id` + `locked_until` say who and until when |
| `success` | all pages fetched and applied; watermark may advance past it |
| `failed` | retries exhausted; needs an alert (see below) |

`retrying` is better modelled as data than as a state — it is just `in_progress` with
`attempt_count > 0`. Collapsing it removes a set of invalid transitions and means the re-claim query
does not have to match two different statuses.

### Claiming — the insert *is* the lock

```sql
INSERT INTO airflow_sync_state
    (project_name, start_time, end_time, status, attempt_count, worker_id, locked_until)
VALUES ($1, $2, $3, 'in_progress', 1, $4, now() + $5)
ON CONFLICT (project_name, start_time, end_time) DO NOTHING;
```

`RowsAffected() == 1` ⇒ this pod owns the window. `0` ⇒ another pod owns it, or it is already
terminal ⇒ skip. No read beforehand, so there is no check-then-act race.

### Re-claiming after a pod dies

Without this, a pod that crashes mid-window leaves the row `in_progress` forever and **every other
pod skips it permanently**, wedging that project's sync:

```sql
UPDATE airflow_sync_state
   SET worker_id = $1, locked_until = now() + $2,
       attempt_count = attempt_count + 1, updated_at = now()
 WHERE project_name = $3
   AND status = 'in_progress'
   AND locked_until < now()
   AND attempt_count < $4
RETURNING id, start_time, end_time;
```

Set `locked_until` above p99 window-processing time. When `attempt_count` reaches the cap, mark
`failed`.

### Completing — fenced

```sql
UPDATE airflow_sync_state
   SET status = 'success', max_processed_log_id = $1, events_matched = $2,
       runs_reconciled = $3, updated_at = now()
 WHERE id = $4 AND worker_id = $5 AND locked_until > now();
```

0 rows ⇒ the lease expired and another pod re-claimed the window ⇒ discard progress and log. This
is why reconciliation writes must be idempotent.

### Watermark, and what to do about a poisoned window

```sql
SELECT max(end_time) FROM airflow_sync_state
 WHERE project_name = $1 AND status IN ('success', 'failed');
```

Including `failed` is a deliberate choice: excluding it would make one permanently-failing window
block the project's sync forever, so *all* later manual overrides go undetected — strictly worse
than a bounded gap. So: retry with backoff, and on exhaustion mark `failed`, **alert**, and let the
watermark move past it. The `failed` rows are the durable record of exactly which intervals were
never reconciled, and can be re-run by resetting their status.

### Serial within a project, parallel across projects

This falls out of the above without extra machinery. Because the next window is always derived from
`max(end_time)` over terminal rows, two pods ticking concurrently compute the *same* window and the
unique constraint picks one winner; the loser skips. A pod cannot run ahead to window N+1 while N is
still `in_progress`, because N is not terminal and so does not move the watermark. Different
projects are independent rows, so they proceed in parallel naturally.

With one project and 5 pods, 4 will no-op each tick. That is fine — the wasted work is a single
failed insert.

**Confirmed against production topology: there are multiple projects, not one**, and the skew
across them is large — one project accounts for the large majority of active jobs, with the rest
ranging down to a small handful each, several of them non-production variants. This matters
because `AirflowSyncConfig` is one global struct applied identically to every project's ticker
cycle — there is no per-project override — so any window/lock/concurrency sizing decision has to
be checked against the *busiest* project, not an average.

It also means the "serial within a project, parallel across projects" property above needed one
more piece to hold up in practice: `tick()` originally iterated every project sequentially in a
single goroutine per pod. With one project this cost nothing; with many, a project whose Airflow
instance was slow or unreachable delayed every project listed after it in that same pod's tick —
for up to `LockDuration` — before the rest even got a chance to run that tick. Fixed by fanning
`tick()` out with a bounded worker pool (new config field `MaxConcurrentProjects`, default 5): the
unique-constraint claim already made cross-project processing *safe* to parallelize, this closes
the gap where one pod's own sequential loop was the actual bottleneck. Verified with a unit test
asserting concurrency stays bounded by `MaxConcurrentProjects` and every project is still
processed exactly once per tick (`core/scheduler/service/airflow_state_sync_worker_test.go`).

A related gap surfaced at the same time: the Airflow HTTP client
(`ext/scheduler/airflow/client.go`) has no request timeout of its own (`&http.Client{}`). A
genuinely hung request would have blocked that window's goroutine forever rather than surfacing
as an attempt error — the lease would still eventually expire and get reclaimed by another
replica, but the original goroutine would leak. `processWindow` now wraps the reconcile call in
`context.WithTimeout(ctx, LockDuration)`, so a stuck call is cancelled in line with when its lease
would lapse anyway.

### Volume: ~99% of fetched rows are discarded, so window size decides feasibility

A production `eventLogs` call filtered to the four events reported `total_entries: 107737`, against
only 626 genuinely-manual `dagrun_*` rows in a comparable period. The reason is V2: worker task
transitions also write bare `success`/`failed`, and there is **no server-side filter for
`extra IS NOT NULL`** (nor a `LIKE` on `owner` — the API's `owner` filter is exact-match only). So
the manual/worker split can only happen client-side, and the discard rate is ~99%.

This is not fatal, because volume scales with window width:

- 107737 rows over ~26 days ≈ **~29 rows per 10-minute window** — a single page.
- But rows are not uniform. Optimus fleets cluster schedules (midnight UTC, top of hour), so a
  peak window can be orders of magnitude above the mean.

Therefore: **implement real pagination** (`offset`, ordered by `event_log_id`, loop until
`total_entries` is exhausted) rather than assuming one page, and size the window from measured peak
rather than mean:

```sql
select date_trunc('hour', dttm) as hour, count(*) from log
where event in ('success','failed','dagrun_success','dagrun_failed')
  and dttm > now() - interval '7 days'
group by 1 order by 2 desc limit 5;
```

Peak-hour count ÷ 6 ≈ rows per 10-minute window; ÷ `[api] maximum_page_limit` (default **100**)
gives the page count for the worst window. If that is unpleasant, options are a shorter window,
raising `maximum_page_limit` on the Airflow side (affects all API consumers), or restricting
server-side to `dagrun_success,dagrun_failed` only — those have **no** worker writer at all, so
they need no client-side discard, at the cost of missing task-level marks.

**Measured against the busiest project in this deployment:**

```
          hour          | count
------------------------+-------
 2026-07-28 03:00:00+07 |  2611
 2026-07-29 03:00:00+07 |  2606
 2026-07-25 03:00:00+07 |  2496
 2026-07-28 12:00:00+07 |  2447
 2026-07-27 03:00:00+07 |  2440
```

≈2,611 rows/hour peak ÷ 6 ≈ 435 rows in the worst 10-minute window ≈ 5 pages at the 100-row page
cap. That confirms the originally-proposed 10-minute window remains safe without shrinking it,
even against the largest project in this deployment — no further tuning needed on window size
itself.

**Indexes.** Confirmed present on this deployment's `log` table: `idx_log_dttm`, `idx_log_event`,
`idx_log_dag` — standard Airflow indexes, not anything added for this feature. No composite
`(dttm, event)` index exists or is recommended: at ~0.02% selectivity (435 of ~2M rows in a
30-day table), the `dttm` range predicate alone is already enough for the planner to pick a cheap
plan (an index scan on `idx_log_dttm`, or a bitmap AND with `idx_log_event`) and filter the
remainder in memory. A composite index would only save filtering a few hundred rows — not worth
adding write overhead to Airflow's own hottest table, or drift outside Airflow's managed schema,
for that. Revisit only if `EXPLAIN ANALYZE` on the real query ever shows a sequential scan.

**Keying: per project or per scheduler host? Resolved: per project, confirmed against production.**
`eventLogs` only filters a **single** `dag_id` (V6), so a per-project worker must fetch the whole
window and map `dag_id` → project client-side; where several Optimus projects share one
`SCHEDULER_HOST` that would mean each project re-fetches the same rows. Production confirms
`SCHEDULER_HOST` is 1:1 with project — many independent projects, each with its own independent
Airflow instance — so per-project keying has no duplication cost here and is the simpler,
failure-isolating choice kept in the final design.

## Point 5 — synchronisation between replicas (revised)

**Do not use `pg_try_advisory_lock`:**

1. The codebase contains no advisory locks anywhere; the established idiom is a lease claim.
2. Session-level advisory locks bind to a specific pooled connection. With `pgxpool`,
   `pg_advisory_unlock` can run on a *different* connection, silently leaking the lock until that
   connection is recycled.
3. `pg_advisory_xact_lock` avoids the leak but forces the cycle into one long transaction, holding
   a connection and generating bloat.

**The window-insert claim above replaces the lease for mutual exclusion.** Because the identifier
is `(project_name, start_time, end_time)`, `INSERT … ON CONFLICT DO NOTHING` gated on
`RowsAffected()` already guarantees exactly-one-replica-per-window, with no lock held and nothing
to release. It is a better fit here than the SLA worker's `UPDATE`-based lease, which assumes a
pre-existing row to claim.

The lease concepts still needed, scoped to the claimed window row:

- `locked_until` — so a crashed replica's `in_progress` window becomes re-claimable rather than
  being lost. Set it above p99 window-processing time.
- `worker_signature` — used to **fence the completion write**, so a replica whose `locked_until`
  expired mid-window cannot mark it complete after another replica re-claimed it:

  ```sql
  UPDATE airflow_sync_state
     SET status = 'completed', completed_at = now(), updated_at = now()
   WHERE project_name = $1 AND start_time = $2 AND end_time = $3
     AND worker_signature = $4
     AND locked_until > now();
  ```

  0 rows affected ⇒ ownership lost mid-window ⇒ discard progress, log, let the re-claimer redo it.
  This is why writes must be idempotent (see edge case 2).

There is one read-then-write race left: two replicas can independently compute the same next window
from `max(end_time)` and both attempt the insert. The unique constraint resolves it — one gets 0
rows and skips. That is correct, not a bug, but the code must treat 0 rows as "someone else has
it", not as an error.

Note `server/optimus.go:247` cancels worker contexts then immediately closes the DB pool without
waiting, so check `ctx.Err()` between pages or every shutdown logs spurious closed-pool errors.

## Point 6 — codebase changes (revised)

| File | Change |
|---|---|
| `migrations/000NNN_create_airflow_sync_state.{up,down}.sql` | see the DDL in "`airflow_sync_state` design" |
| `internal/store/postgres/scheduler/airflow_sync_state_repository.go` | new: `GetWatermark`, `ClaimWindow` (insert/on-conflict), `ReclaimStaleWindow`, `CompleteWindow` (fenced), `FailWindow` |
| `ext/scheduler/airflow/airflow.go` + `client.go` | add `GetManualEventLogs(ctx, tnnt, window, includedEvents, limit, offset)` using the existing `airflowRequest` + `unmarshalAs[T]` style. Must live in `package airflow` (`Client.Invoke` takes the unexported request type). Add a client timeout — the shared `&http.Client{}` has none |
| `ext/scheduler/airflow/model.go` | `EventLog` response struct + `extra` double-unmarshal helper + `toManualOverride()` mapper |
| `internal/store/postgres/scheduler/job_run_repository.go` | additive: lookup by `(project, job_name, scheduled_at)` in batch; batch state update |
| `internal/store/postgres/scheduler/job_operator_repository.go` | additive: fetch/update newest child by `(job_run_id, name)`. Keep the `operatorTypeToTableName` seam |
| `core/scheduler/service/airflow_state_sync_service.go` | new: parse rows → resolve entity → apply write-scope rules |
| `core/scheduler/service/airflow_state_sync_worker.go` | new: ticker + lease + per-host fan-out, modelled on `sla_worker.go:54` |
| `core/scheduler/job_run.go` | shared domain types (`scheduler` cannot import `service`) |
| `core/scheduler/status.go` | Airflow→Optimus state map + ignore-list (see below) |
| `config/config_server.go` | `AirflowSyncConfig`: window interval, lock duration, max concurrent projects, max attempts, exclude-project list. `mapstructure` + `default:` tags (a zero window interval panics `NewTicker`, so it deliberately has none and doubles as the on/off switch). Settling delay, overlap epsilon and max-windows-per-tick are fixed constants in the worker, not config — see "Config surface" below |
| `server/optimus.go` | wire next to the SLA worker (~line 479), guarded by `interval > 0` |
| `config.sample.yaml`, `dev/optimus.values.yaml` | document new keys |

Deviation from the original: a dedicated repository for the window state rather than piling onto
`job_run_repository.go`. Otherwise this now matches the original point-6 layout, since the Airflow
read is REST and belongs in `client.go`/`airflow.go` after all.

**Unresolved questions 3, 5 and 7 from the first draft are now closed:** fetch is via REST, so
there is no Airflow DSN to supply or rotate, and no second connection pool.

### Config surface (revised)

The first implementation exposed eight tunable fields. In practice most of them were either a
fixed correctness margin (an order of magnitude larger than what it protects against) or a value
that could silently contradict another field (an initial lookback set smaller than
`WindowInterval × MaxWindowsPerTick` would never fully apply). Reduced to five:

| Field | Configurable? |
|---|---|
| `window_interval_in_seconds` | yes — also the on/off switch (0 disables the worker) |
| `lock_duration_in_seconds` | yes — depends on this deployment's Airflow latency and event volume |
| `max_concurrent_projects` | yes — depends on project count and pod resources |
| `max_attempts` | yes — how tolerant to be of a flaky Airflow instance is a deployment judgment call |
| `exclude_projects` | yes — see below |
| settling delay | no — fixed at 60s, roughly three orders of magnitude above typical commit latency |
| overlap epsilon | no — fixed at 2s; only needs to cover a boundary tie, not clock skew (settling delay covers that) |
| max windows per tick | no — fixed at 12; a catch-up throttle, not a deployment property |
| initial lookback | not separate — derived as `window_interval × max_windows_per_tick` |

**`exclude_projects`** is new: a list of project names to skip syncing entirely (an unstable
instance, or a project not yet rolled out to this feature). Every project syncs by default —
this is an exclude-list, not an allow-list. Excluding a project simply stops processing it and
freezes its watermark; there's no separate staleness handling for re-inclusion after a long
exclusion — a re-included project's backlog catches up the same way any post-downtime backlog
does, bounded by `max_windows_per_tick` per tick. Kept deliberately simple over a staleness-clamp
alternative.

## Entity resolution

Confirmed against a real API response (2026-07-27), which is worth quoting because it settles
several details at once:

```json
{ "event": "success", "event_log_id": 91255988,
  "execution_date": "2026-07-23T00:00:00+00:00", "extra": null,
  "owner": "hilda.huang@test.com",
  "task_id": "wait_project.schema.name-mc2mc",
  "when": "2026-07-24T00:00:04.497221+00:00" }          // worker row

{ "event": "success", "event_log_id": 91885625,
  "execution_date": null,
  "extra": "{\"confirmed\": \"true\", \"past\": \"false\", ...}",
  "owner": "google_109294631035232612844", "task_id": "mc2mc",
  "run_id": "scheduled__2026-07-24T18:00:00+00:00",
  "when": "2026-07-26T09:38:16.170441+00:00" }          // manual row
```

- **`execution_date` is populated on worker rows but NULL on manual rows** — so the manual path
  must resolve time from `run_id` only. (`action_logging` sets `execution_date` only when the
  request carries it, and the legacy mark form sends `dag_run_id`.) Do not be misled into using
  `execution_date` because it is present on the rows you are filtering *out*.
- `task_id` prefixes behave as expected: `wait_…` (sensor) and bare `mc2mc` (task).
- `when` is normalised to **UTC** by the API even though `dttm` renders as `+07` in psql, so send
  `after`/`before` as explicit UTC ISO8601.
- `extra` arrives as a **JSON-encoded string**, confirming the double-unmarshal.
- Worker rows also carry an `owner` (the DAG owner — an email here, not an `@handle`). So `owner`
  identifies the *actor* only when `extra` is populated; never present it as "who did this"
  otherwise.

1. `dag_id` → Optimus job. On a shared Airflow, `dag_id` is only unique per project — resolve via
   the project owning that `SCHEDULER_HOST`, and no-op on unknown DAGs.
2. `run_id` → `scheduled_at`. `execution_date` is **NULL** in these rows; only `run_id` is
   populated, and it comes in at least three shapes seen in production:
   - `scheduled__2026-07-24T18:00:00+00:00`
   - `custom-backfill_UUID__2026-07-25T09:38:53+00:00`
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
- **Emit state-change events, but suppress user-facing alerts.** Call
  `raiseJobRunStateChangeEvent` (`core/scheduler/service/job_run_service.go:960`) so downstream
  consumers don't desync — see "Alerting" below for why the notification itself should be
  suppressed, and the existing mechanism for doing it.

## Alerting on reconciled changes

Reconciliation mutates `job_run.status`, which is an input to notifications, the SLA worker and the
potential-SLA-breach predictor. Writing state without deciding this produces surprising pages:

| Scenario | Naive behaviour | Wanted |
|---|---|---|
| Optimus `failed` → human marked success → reconciler writes `success` | fires a "job success" notification hours late | no notification; ideally *resolve* the earlier failure alert if Siren's firing/resolved semantics are used |
| Optimus `running` → human marked the DAG failed → reconciler writes `failed` | fires a fresh failure alert for a failure the human caused deliberately and already knows about | no notification |
| Stale `running` → reconciled to a terminal state | the SLA predictor may immediately raise a breach for a run that ended hours ago | no breach alert |

In every case the state change originates from a deliberate human action **inside Airflow**, so the
person who caused it already knows. Recommendation for v1: **update state and emit the internal
state-change event, but do not notify.** Surface reconciliation through a metric
(`airflow_state_reconciled_total{event,from_state,to_state}`) and a log line carrying `owner`, so
the team can see it without anyone being paged.

There is already an idiomatic mechanism: `scheduler.Event` carries a **`SkipAlerting bool`**
(`core/scheduler/event.go`), used today by `filterSLAObjects`
(`core/scheduler/service/job_run_service.go:867`). Reuse it rather than inventing a bypass.

Deliberately deferred: mapping `failed`→`success` onto an alert *resolution* is desirable but needs
Siren's resolve semantics wired up, so treat it as a follow-up rather than v1.

## Edge cases

**Correctness**
1. **Late-committing rows.** A row's `dttm` is stamped at request time but becomes visible only on
   commit, so a window queried too soon can miss rows that belong to it — and the window then
   advances past them forever. The settling delay in "Windowing" item 1 is the mitigation; it is
   not optional.
2. Ordering — apply rows in ascending `id`, and make writes idempotent so a replayed cycle is a
   no-op. Production data shows genuinely messy sequences (a `dagrun_success`, then a task
   `success`, then another `dagrun_success` 20s apart on the same run).
3. **Clock skew.** Windows are computed from Optimus's clock but filter on Airflow's `dttm`. If
   Airflow's clock runs behind, freshly-written rows fall below a window Optimus already closed.
   The settling delay absorbs small skew; larger skew needs monitoring. Also send explicit
   UTC offsets in `after`/`before` — `timezone.parse()` will accept a naive string and assume a
   default, and production `dttm` renders as `+07`.
4. `airflow db clean` truncates `log`; an empty window is normal and must not be treated as an
   error or block window advancement.
4b. **Pagination.** `limit` is capped by `[api] maximum_page_limit` (default 100). Page with
   `offset` ordered by `event_log_id`, and use the returned `total_entries` to know when to stop —
   a window with more rows than one page is otherwise silently truncated.
5. `map_index` (dynamic task mapping) is not modelled by Optimus's child tables — ignore
   explicitly rather than by accident.

**Scope / mapping**
6. **v1 processes all four observed run_id shapes**: `scheduled__*`, `manual__*`, `replayed__*`,
   `custom-backfill_*`. Implemented as a positive `run_id` prefix allow-list
   (`scheduler.HasRecognisedRunIDPrefix`), not a deny-list, so an unrecognised future prefix is
   skipped-and-counted rather than mis-parsed.

   This was initially narrowed to `scheduled__*` only, over a **replay collision** concern:
   Optimus's replay worker actively writes state for `replayed__` runs, and production data showed a
   human marking such a run success — apparently two writers on one row. That concern didn't survive
   closer reading of the replay/backfill code: `replayRepo`/`backfillRepo` only ever write to their
   own tracking tables (`replay_request`/`replay_run`, `backfill`); `job_run`/`task_run`/`sensor_run`/
   `hook_run` are updated exclusively through the normal Airflow callback path
   (`__lib.py notify_event` → `RegisterJobEvent` → `UpdateJobState`) regardless of whether a run was
   scheduled, replayed, or backfilled. So there is no second writer to arbitrate against — the only
   race is the same normal-callback-vs-manual-override race that already exists for `scheduled__`
   runs, already covered by the `updated_at` freshness check (see "Write-scope rules").

   The croniter shift (`ExecutionDate` → `Schedule.GetNextSchedule`) is identical across all four
   shapes once the run_id's embedded timestamp is extracted — verified against a live instance for
   `scheduled__` (see "Demo" below) and holds by construction for the others, since it's the same
   `execution_date → next tick` arithmetic `__lib.py get_scheduled_at` and
   `ext/scheduler/airflow/client.go`'s `getJobRuns` both already use elsewhere in this codebase.
   `custom-backfill_UUID__TIMESTAMP` is the one irregular shape: split on the **last** `__`, not
   a fixed prefix strip, since the UUID's hyphens never produce a literal `__` — confirmed against a
   real run_id (`custom-backfill_76515cc3-b3aa-440f-b998-04e6f4935ea3__2026-07-25T09:38:53+00:00`).

   `manual__*` runs frequently have no `job_run` row in Optimus at all (an ad-hoc trigger with no
   corresponding schedule); this needs no special case — the existing `GetByScheduledAt` → NotFound →
   skip (`reason="no_job_run"`) path already handles it, since v1 never creates rows.

   **Count what you skip.** `airflow_state_reconcile_skipped_total{reason="run_type"}` still exists
   for whatever prefix shows up beyond these four. Otherwise "no manual overrides found" is
   indistinguishable from "N overrides found on an unrecognised run_id shape and all were dropped" —
   and since the feature exists to remove false-positive alerts, a silently-skipped override means
   the false positive persists.

6b. **Demo (2026-07-28), confirms the croniter shift end-to-end on real infrastructure**: marked a
   real `scheduled__2026-07-27T05:00:00+00:00` Airflow run failed via the actual grid UI (not a
   simulated audit row). The reconciler correctly computed Optimus's `job_run.scheduled_at` as
   `2026-07-28T05:00:00` (next tick after the run_id's embedded timestamp, per the job's `0 5 * * *`
   interval) and flipped that row from `success` to `failed`. Its `task_run` child, already terminal
   (`success`) before the override, was correctly left untouched — the asymmetric-cascade rule
   (finding V5) confirmed live: mirroring `set_dag_run_state_to_failed`, which never rewrites an
   already-finished task instance.

6c. **Clear operations are out of scope by construction**, since `clear` / `dagrun_clear` (and their
   REST equivalents) are not in the four-event filter — nothing to implement. That is the right
   default: a cleared task re-runs and emits normal callbacks, so Optimus self-heals. What is given
   up is the case where a cleared task never re-runs — paused DAG, exhausted pool, or the task
   removed from the DAG — leaving stale state indefinitely. Supporting it later is not just a filter
   change: Airflow clears to `None`, which has no Optimus equivalent.
7. Renamed/deleted jobs and non-Optimus DAGs on a shared Airflow must no-op quietly.
8. Task-group marks (`group_id` instead of `task_id`) hit the same endpoints and log `task_id`
   NULL — detect and skip, or expand.

**Operational**
9. One unreachable Airflow DB must not starve other projects — isolate per host and still advance
   the others.
10. First run against a long backlog could be enormous — mitigated by deriving the initial
    lookback from `window_interval × max_windows_per_tick` (see "Config surface"), so it can never
    exceed one tick's catch-up budget.
11. **Skipping a project.** Not wired to `DISABLE_JOB_SCHEDULING` — that remains unimplemented.
    Instead, `exclude_projects` (see "Config surface") is an explicit, manually-maintained
    exclude-list. Revisit tying it to `DISABLE_JOB_SCHEDULING` if manual maintenance proves to be
    a real operational burden.
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

# Decisions taken

- Fetch via REST `eventLogs`; no Airflow-DB dependency.
- Manual detection: four bare events + `extra` populated + `confirmed='true'`, with `owner LIKE
  'google_%'` as a confirmed-safe additional signal in this deployment.
- Windowed sync state keyed by `(project_name, start_time, end_time)`, claimed by
  `INSERT … ON CONFLICT DO NOTHING`.
- `SCHEDULER_HOST` is 1:1 with project here, so keying by project is correct.
- **v1 scope is all four observed run_id shapes** (`scheduled__*`, `manual__*`, `replayed__*`,
  `custom-backfill_*`), via a positive allow-list. Only `clear` operations are out, by construction
  (not in the four-event filter). The initial `scheduled__*`-only narrowing was reversed after
  establishing the replay/backfill workers never write to `job_run`/`task_run` themselves (see
  finding 6 above) — there was no actual collision to avoid.
- Reconciliation updates state and emits the internal job-run-state-change event
  (`emitJobRunStateChange`, mirroring `raiseJobRunStateChangeEvent`) but never calls the gRPC
  handler's notifier fan-out (`ext/notify/alertmanager` etc.) at all, so no user-facing alert is
  possible by construction — not something gated by `Event.SkipAlerting` (that flag lives on the
  inbound-callback `scheduler.Event` used by `EventsService`/SLA-miss filtering, a different code
  path this reconciler never goes through, since it calls repositories directly rather than
  replaying a synthetic event through `JobRunService.UpdateJobState`).
- `airflow_sync_state` carries `max_processed_log_id` (from the API's `event_log_id`) plus
  `events_matched` / `runs_reconciled` for observability.
- Bulk-flagged rows (`past`/`future`/`upstream`/`downstream` = `true`) are scoped out in v1: counted
  in `airflow_state_reconcile_skipped_total{reason="bulk_flag"}`, not applied. A single such row can
  mean many changed task instances, possibly in other DAG runs, which the audit log cannot
  enumerate — attempting to apply it risks acting on an incomplete/wrong picture, so skip-and-count
  is the safe default. Re-visit only if the metric shows material volume.
- **Bounded per-tick project concurrency.** Production has many projects, not the one assumed while
  reasoning about "serial within a project, parallel across projects" above — `tick()` now fans out
  across projects with a bounded worker pool (`MaxConcurrentProjects`, default 5) instead of a
  single sequential loop, so one slow/unreachable project's Airflow instance no longer delays every
  project listed after it within the same pod's tick.
- **Per-window reconcile timeout.** `processWindow` bounds the reconcile call to
  `context.WithTimeout(ctx, LockDuration)`, since the Airflow HTTP client has no timeout of its own
  and would otherwise leak a goroutine per genuinely-hung request instead of surfacing as a
  retryable attempt error.
- New metric `airflow_sync_windows_failed_total{project}`: `FailExhaustedWindows` giving up on a
  window was previously only a log line — with many independent projects/Airflow instances as
  separate failure domains, that needed to be alertable per project, not just grep-able.
- **Config surface reduced from eight fields to five** (see "Config surface" above): settling
  delay, overlap epsilon and max-windows-per-tick are now fixed constants, and initial lookback is
  derived rather than separately set. Added `exclude_projects`, a manual exclude-list for projects
  that shouldn't sync (unstable instance, not yet rolled out).

All items in this section are now decided; nothing left blocking implementation.

# Unresolved questions

1. REST-driven (`api.*`) overrides from non-Optimus tooling — in scope? Not blocking v1 (the filter
   already excludes them by construction); revisit if a real need surfaces.
2. Follow-up: should `failed`→`success` *resolve* an existing Siren alert rather than being silent?
