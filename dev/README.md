
# Optimus Dev setup

Full local end-to-end environment: colima (local Kubernetes) + Optimus server + Optimus DB + real Airflow + Airflow DB. Task pods run for real via `KubernetesPodOperator`, so this is a genuine end-to-end test environment, not a mock.

## 1. Start colima

+ `brew install colima`
+ `make start-colima`
+ check kubernetes context is `colima`: `kubectl config current-context`

## 2. Build and load the Optimus images

The chart expects a **local** image (`optimus-dev:latest`, `pullPolicy: IfNotPresent`) — it does not build from source itself.

```sh
# from the repo root
GOOS=linux GOARCH=arm64 go build -o optimus .   # match your Mac's arch; use amd64 on Intel
docker build -t optimus-dev:latest .
```

Task pods also spin up an **init container** to fetch/compile job assets. Its image name is hardcoded in the DAG template as `gotocompany/optimus:{{.Version}}`, where `{{.Version}}` defaults to `"dev"` for a binary built without the Makefile's version ldflags. Since this init container only does asset compilation (unrelated to any server-side logic), it's safe to reuse the image you just built:

```sh
docker tag optimus-dev:latest gotocompany/optimus:dev
```

You'll also want a **native macOS CLI binary** (separate from the Linux one above) to actually run `optimus` commands from your terminal:

```sh
go build -o /tmp/optimus-cli .
```

## 3. Deploy

+ `cd dev`
+ `make apply`
+ port-forward (needed again any time a pod restarts — see [Troubleshooting](#troubleshooting)):
    + `kubectl port-forward svc/optimus-dev 9100:80 -n optimus-dev` (HTTP gateway)
    + `kubectl port-forward svc/optimus-dev 9101:8081 -n optimus-dev` (gRPC — needed for CLI commands, see below)
    + `kubectl port-forward svc/airflow-webserver 8080:8080 -n optimus-dev`

Some optional variables for `make apply`:
```sh
DAGS_PATH=                # default /tmp/colima/dags
OPTIMUS_SERVE_PORT=       # default 9100
SETUP_FILE_PATH=          # default ./setup.yaml
```

## Components
+ optimus server
+ optimus db (postgres)
+ airflow
+ airflow db (postgres)

### Dag file location on your laptop
+ `/tmp/colima/dags` or specified by `DAGS_PATH`

## 4. Spinning up a project

CLI commands need `OPTIMUS_INSECURE=1` (no TLS locally, so the CLI skips the client-auth check it otherwise requires) and must target the **gRPC** port (`9101`), not the HTTP one:

```sh
mkdir my-project && cd my-project
```

Write `optimus.yaml` by hand (there's no interactive `optimus init` shortcut that's faster than this):
```yaml
version: 1
host: localhost:9101
project:
  name: my-project
  preset_path: presets.yaml   # see note below — required if any job uses `window: preset: X`
  config:
    storage_path: file:///airflow-dags       # NOT /airflow-dags/dags -- see note below
    scheduler_host: airflow-webserver:8080   # in-cluster Airflow service DNS name
    scheduler_version: "2.9.3"                # must match airflow.values.yaml's airflowVersion
    ALERTMANAGER_TEAM: <real team name>       # only used for alert routing; see step 6
namespaces:
- name: my-ns
  job:
    path: jobs
```

```sh
OPTIMUS_INSECURE=1 /tmp/optimus-cli project register -c optimus.yaml
OPTIMUS_INSECURE=1 /tmp/optimus-cli namespace register -c optimus.yaml --name my-ns
```

**`storage_path` gotcha**: Optimus's own DAG writer appends its own `dags/` subfolder on top of whatever `storage_path` you give it. The chart mounts the shared dags volume at `/airflow-dags/dags` inside the Optimus pod — if you set `storage_path: file:///airflow-dags/dags` (the mount point itself), files land double-nested at `.../dags/dags/...` and Airflow never sees them. Use `file:///airflow-dags` (one level up) instead.

**`preset_path` gotcha**: if any job spec uses `window: preset: SOMENAME`, the project needs a matching `presets.yaml` registered, e.g.:
```yaml
presets:
  YESTERDAY:
    description: yesterday's data
    window:
      size: 1d
      shift_by: 0d
      truncate_to: d
```
Without it, `job replace-all` fails with `preset not found <name>`.

### Registering a scheduler role (needed before jobs can run)

Airflow's REST API requires basic auth (default `admin`/`admin` per the Helm chart's own install notes). Register it as a project secret **before** the namespace's scheduler role can be created:
```sh
OPTIMUS_INSECURE=1 /tmp/optimus-cli secret set SCHEDULER_AUTH "admin:admin" -p my-project --host localhost:9101 --confirm
OPTIMUS_INSECURE=1 /tmp/optimus-cli namespace register -c optimus.yaml --name my-ns   # re-run to create the role
```
(`ALERTMANAGER_TEAM` is project **config**, not a secret — put it in `optimus.yaml` directly, not via `optimus secret set`.)

### Mounting the plugins
+ define plugin artifacts in `setup.yaml` under `plugins:` (either a URL or a local `./plugins/optimus-plugin-*.yaml` path)
+ `make apply.optimus` picks these up and copies local files into the shared plugins volume
+ **note**: the actual plugin manifest for the `python` task type (used by most jobs in this repo) is at `dev/plugins/optimus-plugin-python.yaml` — a plain `python:3.8-slim-bullseye` image with a generic wrapper script, no private image needed.

## 5. Deploying jobs

```sh
OPTIMUS_INSECURE=1 /tmp/optimus-cli job replace-all -c optimus.yaml -N my-ns -v
```

There is no `optimus plugin sync` command in this CLI version (older docs may mention it) — plugin registration is entirely server-side via the Helm deploy's `OPTIMUS_PLUGIN_ARTIFACTS`, nothing needed client-side.

If a job spec's own config (env vars, SLA duration, etc.) changed but the DAG file on disk looks stale, force a re-upload:
```sh
OPTIMUS_INSECURE=1 /tmp/optimus-cli scheduler upload-all -c optimus.yaml
```

**New DAGs are paused by default.** Unpause them, or nothing will ever run:
```sh
curl -u admin:admin -X PATCH "http://localhost:8080/api/v1/dags/<dag_id>" \
  -H "Content-Type: application/json" -d '{"is_paused": false}'
```

## Troubleshooting

- **`kubectl port-forward` dies after one request** ("error: lost connection to pod"): this is normal `kubectl port-forward` behavior on a connection hiccup — just re-run the port-forward command. Any pod restart (e.g. after `make apply.optimus`) also invalidates existing forwards.
- **CLI gRPC calls fail with "Unable to reach optimus server"**: the CLI talks gRPC directly, not through the HTTP gateway. Forward the **gRPC** service port, not the HTTP one: `kubectl port-forward svc/optimus-dev 9101:8081 -n optimus-dev`.
- **`serve.ingress_host_grpc: required config missing` (crash loop)**: `configYaml` is a raw string in the Helm values — providing it at all replaces the chart's own defaults wholesale rather than merging. Make sure `serve.ingress_host_grpc`, `sla.worker_interval_minutes`/`worker_lock_duration_minutes`, and `plugins.location: /app/plugins` are all explicitly set (already done in this repo's `optimus.values.yaml` — if you see this error, something likely overrode `configYaml` without carrying these forward).
- **`panic: non-positive interval for NewTicker`**: same root cause as above — `sla.worker_interval_minutes` defaulted to `0`.
- **Task init container `ImagePullBackOff` on `gotocompany/optimus:dev`**: see step 2 — tag your locally built image to that name.
- **gRPC calls succeed but nothing happens / `job_run_id` mismatch weirdness**: the Helm chart hardcodes the pod's gRPC container port to `9101` regardless of app config — make sure `serve.port_grpc: 9101` in `configYaml` matches (already set in this repo's values file).
- **`plugins(0)` in server logs (jobs fail with "unsupported plugin requested")**: `plugins.location` wasn't set (defaults to a `.plugins`-prefixed dir that doesn't exist), while the chart mounts plugin YAMLs at `/app/plugins` (no dot). Set `plugins.location: /app/plugins` explicitly.
- **DAG import error `ModuleNotFoundError: No module named '__lib'`**: `__lib.py` is written to the DAGs bucket root once per server-process-lifetime (in-memory dedup flag) — if it was written once to a wrong path (e.g. before fixing the `storage_path` double-nesting bug) and then the path was corrected, the flag still thinks it's synced. Restart the `optimus-dev` pod to clear it, then re-run `scheduler upload-all`.

### Connect to optimus db
+ `psql -h localhost -U optimus`

### Connect to the DB directly for debugging
```sh
kubectl exec -n optimus-dev deploy/optimus-db -- psql -U optimus -d optimus -c "SELECT job_name, scheduled_at, start_time, end_time, status FROM job_run WHERE job_name='<job_name>' ORDER BY scheduled_at DESC LIMIT 10;"
```

### Load secrets
+ define key value pair of secrets on `setup.yaml` under section `secrets`
+ `make _load.secrets`

Some optional variable you can set alongside with `make _load.secrets`
```sh
SETUP_FILE_PATH= # default ./setup.yaml
PROJECT=         # default project-a
HOST=            # default localhost:9100
```
