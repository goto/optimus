import json
import logging
from datetime import datetime, timezone, timedelta

from typing import Any, Dict, Optional
from dataclasses import dataclass, asdict

import pendulum
import requests
from airflow.configuration import conf
from airflow.hooks.base import BaseHook
from airflow.models import (Variable, XCom, TaskReschedule )
try:
    from airflow.models import XCOM_RETURN_KEY
except ImportError:
    from airflow.utils.xcom import XCOM_RETURN_KEY  # airflow >= 2.5
try:
    is_support_init_container_logs = False
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
except ImportError:
    is_support_init_container_logs = True
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator # providers-cncf-kubernetes >= 10.4.3
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import TaskInstanceState
from airflow.exceptions import AirflowException
from croniter import croniter
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# UTC time zone as a tzinfo instance.
utc = pendulum.timezone('UTC')

DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIMESTAMP_MS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

SCHEDULER_ERR_MSG = "scheduler_error"
STARTUP_TIMEOUT_IN_SECS = int(Variable.get("startup_timeout_in_secs", default_var=2 * 60))
OPTIMUS_REQUEST_TIMEOUT_IN_SECS = int(Variable.get("optimus_request_timeout_in_secs", default_var=5 * 60))

# --- EVENT TYPES -----------------------------------
OPERATOR_START_EVENT      = "operator_start"
OPERATOR_RETRY_EVENT      = "operator_retry"
OPERATOR_SUCCESS_EVENT    = "operator_success"
OPERATOR_FAIL_EVENT       = "operator_fail"

JOB_SUCCESS_EVENT   = "job_success"
JOB_FAIL_EVENT      = "job_fail"
JOB_SLA_MISS_EVENT  = "job_sla_miss"
# ---------------------------------------------------

def lookup_non_standard_cron_expression(expr: str) -> str:
    expr_mapping = {
        '@yearly': '0 0 1 1 *',
        '@annually': '0 0 1 1 *',
        '@monthly': '0 0 1 * *',
        '@weekly': '0 0 * * 0',
        '@daily': '0 0 * * *',
        '@midnight': '0 0 * * *',
        '@hourly': '0 * * * *',
    }
    try:
        return expr_mapping[expr]
    except KeyError:
        return expr

def get_scheduled_at(context):
    interval = context.get("dag").schedule_interval
    if interval is None:
        # pendulum.Datetime cannot work with serializer used by airflow, so need to convert to datetime
        return datetime.fromtimestamp(context.get('logical_date').timestamp(), tz=utc)

    job_cron_iter = croniter(interval, context.get('execution_date'))
    return job_cron_iter.get_next(datetime)

class SuperKubernetesPodOperator(KubernetesPodOperator):
    def __init__(self, *args, **kwargs):
        kwargs["startup_timeout_seconds"] = STARTUP_TIMEOUT_IN_SECS
        kwargs["log_events_on_failure"] = True
        if is_support_init_container_logs:
            kwargs["init_container_logs"] = True
        super(SuperKubernetesPodOperator, self).__init__(*args, **kwargs)
        self.do_xcom_push = kwargs.get('do_xcom_push')
        self.namespace = kwargs.get('namespace')
        self.in_cluster = kwargs.get('in_cluster')
        self.cluster_context = kwargs.get('cluster_context')
        self.reattach_on_restart = kwargs.get('reattach_on_restart')
        self.config_file = kwargs.get('config_file')

    def render_init_containers(self, context):
        for index, ic in enumerate(self.init_containers):
            env = getattr(ic, 'env')
            if env:
                self.init_containers[index].env.append(k8s.V1EnvVar(name="SCHEDULED_AT", value=get_scheduled_at(context)))
                self.render_template(env, context)

    def execute(self, context):
        self.render_init_containers(context)
        # call KubernetesPodOperator to handle the pod
        return super().execute(context)


class OptimusAPIClient:
    def __init__(self, optimus_host):
        self.host = self._add_connection_adapter_if_absent(optimus_host)

    def _add_connection_adapter_if_absent(self, host):
        if host.startswith("http://") or host.startswith("https://"):
            return host
        return "http://" + host

    def get_job_replay_config(self, project_name,  job_name, schedule_time) -> dict:
        scheduled_at_str = schedule_time.strftime(TIMESTAMP_FORMAT)
        log.info("Getting replay config for job:{}, project:{}, schedule_time:{}".format(job_name, project_name, scheduled_at_str))
        url = '{optimus_host}/api/v1beta1/project/{optimus_project}/replay-details'.format(
            optimus_host=self.host,
            optimus_project=project_name,
        )
        response = requests.get(url, params={
            'scheduled_at': scheduled_at_str,
            'job_names':    job_name,
            'status':       "in progress",
        }, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        log.info("get replay details request URL {}".format(response.url))
        if response.status_code != 200:
            log.error("Received non 200 status code:{}, response:{}".format(response.status_code, response))
            return {}
        replay_config = response.json()
        log.info("Received replay config :{}".format(replay_config))
        if 'replays' in replay_config.keys():
            if len(replay_config['replays']) == 0 :
                log.warning("unable to find a 'in_progress' replay for the given scheduled date:{}"
                            .format(scheduled_at_str))
                return {}
            for config in replay_config['replays']:
                if 'replayConfig' in config.keys():
                    if 'jobConfig' in config['replayConfig'].keys():
                        log.info("Found a replay with specified 'jobConfig' for the given scheduled_date:{}, config:{}"
                                 .format(scheduled_at_str, config))
                        return config['replayConfig']['jobConfig']
        return {}

    def get_job_run(self, project_name: str, job_name: str, start_date: str, end_date: str, downstream_project_name: str, downstream_job_name: str) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{optimus_project}/job/{optimus_job}/run'.format(
            optimus_host=self.host,
            optimus_project=project_name,
            optimus_job=job_name,
        )
        response = requests.get(url, params={
            'start_date': start_date,
            'end_date': end_date,
            'downstream_project_name': downstream_project_name,
            'downstream_job_name': downstream_job_name
        }, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        self._raise_error_if_request_failed(response)
        return response.json()

    def execute_third_party_sensor(self, project_name: str, job_name: str, third_party_type: str, scheduled_at: str, identifier: str, config: dict) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{optimus_project}/job/{optimus_job}/third-party-sensor'.format(
            optimus_host=self.host,
            optimus_project=project_name,
            optimus_job=job_name
        )
        log.info("Executing third party sensor for project_name: {}, job_name: {}, identifier: {}, third_party_type: {}, scheduled_at: {}".format(project_name, job_name, identifier, third_party_type, scheduled_at))
        payload = {'scheduled_at': scheduled_at, 'third_party_type': third_party_type, 'identifier': identifier}

        if third_party_type == 'dex':
            resource_urn = config.get('resource_urn', "")
            payload['dex_sensor_request'] = {
                'resource_urn': resource_urn,
            }

        response = requests.put(url, json=payload,
                                timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        self._raise_error_if_request_failed(response)
        log.info("Request Body: {}".format(response.request))
        return response.json()


    def get_task_window(self, project_name: str, job_name: str, scheduled_at: str) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{project_name}/job/{job_name}/interval?reference_time={reference_time}'.format(
            optimus_host=self.host,
            project_name=project_name,
            job_name=job_name,
            reference_time=scheduled_at,
        )
        response = requests.get(url, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        self._raise_error_if_request_failed(response)
        return response.json()


    def get_job_run_input(self, execution_date: str, project_name: str, job_name: str, job_type: str, instance_name: str) -> dict:
        response = requests.post(url="{}/api/v1beta1/project/{}/job/{}/run_input".format(self.host, project_name, job_name),
                      json={'scheduled_at': execution_date,
                            'instance_name': instance_name,
                            'instance_type': "TYPE_" + job_type.upper()}, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)

        self._raise_error_if_request_failed(response)
        return response.json()

    def get_jobs(self, project, job) -> dict:
        url = '{optimus_host}/api/v1beta1/jobs?project_name={project_name}&job_name={job_name}'.format(
            optimus_host=self.host,
            project_name=project,
            job_name=job)
        response = requests.get(url, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        self._raise_error_if_request_failed(response)
        return response.json()

    def get_job_metadata(self, namespace, project, job) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{project_name}/namespace/{namespace_name}/job/{job_name}'.format(
            optimus_host=self.host,
            namespace_name=namespace,
            project_name=project,
            job_name=job)
        response = requests.get(url, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        self._raise_error_if_request_failed(response)
        return response.json()

    def notify_event(self, project, namespace, job, event) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{project_name}/namespace/{namespace}/job/{job_name}/event'.format(
            optimus_host=self.host,
            project_name=project,
            namespace=namespace,
            job_name=job,
        )
        request_data = {
            "event": event
        }
        response = requests.post(url, data=json.dumps(request_data), timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        self._raise_error_if_request_failed(response)
        return response.json()

    def _raise_error_if_request_failed(self, response):
        if response.status_code != 200:
            log.error("Request to optimus returned non-200 status code. Server response:\n")
            log.error(response.json())
            raise AssertionError("request to optimus returned non-200 status code. url: " + response.url)


class JobSpecTaskWindow:
    def __init__(self, optimus_client: OptimusAPIClient, project_name: str, job_name: str):
        self._optimus_client = optimus_client
        self.project_name = project_name
        self.job_name = job_name

    def get(self, scheduled_at: str) -> (datetime, datetime):
        api_response = self._fetch_task_window(scheduled_at)
        return (
            self._parse_datetime(api_response['startTime']),
            self._parse_datetime(api_response['endTime']),
        )

    # window start is inclusive
    def get_schedule_window(self, scheduled_at: str, upstream_schedule: str) -> (str, str):
        api_response = self._fetch_task_window(scheduled_at)

        schedule_time_window_start = self._parse_datetime(api_response['startTime'])
        schedule_time_window_end = self._parse_datetime(api_response['endTime'])

        job_cron_iter = croniter(upstream_schedule, schedule_time_window_start)
        schedule_time_window_inclusive_start = job_cron_iter.get_next(datetime)
        return (
            self._parse_datetime_utc_str(schedule_time_window_inclusive_start),
            self._parse_datetime_utc_str(schedule_time_window_end),
        )

    def _parse_datetime(self, timestamp):
        return datetime.strptime(timestamp, TIMESTAMP_FORMAT)

    def _parse_datetime_utc_str(self, timestamp):
        return timestamp.strftime(TIMESTAMP_FORMAT)

    def _fetch_task_window(self, scheduled_at: str) -> dict:
        return self._optimus_client.get_task_window(self.project_name, self.job_name, scheduled_at)

def check_dry_run_config(response):
    accepted_values = {"true", "yes", "y"}

    # Check if 'jobSpecificationResponses' exists and is a non-empty list
    job_specs = response.get("jobSpecificationResponses")
    if not isinstance(job_specs, list) or len(job_specs) == 0:
        print("Missing or empty 'jobSpecificationResponses'")
        return False

    first_spec = job_specs[0]
    if not isinstance(first_spec, dict):
        print("First jobSpecificationResponse is not a valid object")
        return False

    # Check if 'job' exists and is a dictionary
    job = first_spec.get("job")
    if not isinstance(job, dict):
        print("Missing or invalid 'job' in first jobSpecificationResponse")
        return False

    # Check if 'config' exists and is a list
    config_list = job.get("config")
    if not isinstance(config_list, list):
        print("Missing or invalid 'config' in job")
        return False

    for item in config_list:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if name == "DRY_RUN" and isinstance(value, str):
            if value.strip().lower() in accepted_values:
                return True

    return False

class SuperExternal3rdPartyTaskSensor(BaseSensorOperator):
    def __init__(
            self,
            optimus_hostname: str,
            project_name: str,
            job_name: str,
            third_party_type: str,
            identifier: str,
            config: dict,
            *args,
            **kwargs) -> None:
        kwargs['mode'] = kwargs.get('mode', 'reschedule')
        super().__init__(**kwargs)
        self.project_name = project_name
        self.job_name = job_name
        self.third_party_type = third_party_type
        self.identifier = identifier
        self.config = config
        self._optimus_client = OptimusAPIClient(optimus_hostname)
        self._third_party_types_supported = ["dex"]

    def poke(self, context):
        self.log.info("Poking for third party upstream '{}' for identifier '{}'".format(self.third_party_type, self.identifier))
        schedule_time = get_scheduled_at(context)
        self.log.info("Current schedule_time: {}".format(schedule_time))

        if self.third_party_type not in self._third_party_types_supported:
            self.log.warning("third party type '{}' not supported, skipping sensor check".format(self.third_party_type))
            return True

        if not self.is_upstream_data_available(schedule_time):
            self.log.warning("upstream data not yet available for third party '{}' for identifier '{}' at schedule_time '{}', rescheduling sensor".
                             format(self.third_party_type, self.identifier, schedule_time))
            return False
        return True

    def is_upstream_data_available(self, schedule_time) -> bool:
        try:
            log.info("logging parameters ")
            log.info("project_name          : {}".format(self.project_name))
            log.info("job_name              : {}".format(self.job_name))
            log.info("schedule_time         : {}".format(schedule_time))
            log.info("third_party_type      : {}".format(self.third_party_type))
            log.info("identifier            : {}".format(self.identifier))

            if self.third_party_type == "dex":
                api_response = self._optimus_client.execute_third_party_sensor(self.project_name, self.job_name, self.third_party_type, schedule_time, self.identifier, self.config)
                self.log.info("job_run api response :: {}".format(api_response))
                print("api_response: ", api_response)

                # dex specific response parsing
                resp = api_response.get('dex_sensor_response', {})
                if resp['is_complete']:
                    log.info("optimus response :: {}".format(resp))
                    return True
                return False
            else :
                log.warn("third party type other than 'dex' detected : {}".format(self.third_party_type))
                return False

        except Exception as e:
            self.log.warning("error while processing sensor :: {}".format(e))
            raise AirflowException(e)



class SuperExternalTaskSensor(BaseSensorOperator):

    def __init__(
            self,
            optimus_hostname: str,
            project_name: str,
            job_name: str,
            upstream_optimus_hostname: str,
            upstream_optimus_project: str,
            upstream_optimus_namespace: str,
            upstream_optimus_job: str,
            *args,
            **kwargs) -> None:
        kwargs['mode'] = kwargs.get('mode', 'reschedule')
        super().__init__(**kwargs)
        self.project_name = project_name
        self.job_name = job_name
        self.upstream_optimus_project = upstream_optimus_project
        self.upstream_optimus_namespace = upstream_optimus_namespace
        self.upstream_optimus_job = upstream_optimus_job
        self._optimus_client = OptimusAPIClient(optimus_hostname)
        self._upstream_optimus_client = OptimusAPIClient(upstream_optimus_hostname)

    def poke(self, context):
        try:
            jobs_obj = self._optimus_client.get_jobs(self.project_name, self.job_name)
            if check_dry_run_config(jobs_obj):
                log.info("Bypassing upstream check as job configured in DRY_RUN mode")
                return True
        except Exception as e:
            self.log.warning("error while fetching upstream schedule :: {}".format(e))

        schedule_time = get_scheduled_at(context)
        job_config = self._optimus_client.get_job_replay_config(self.project_name, self.job_name, schedule_time)
        if 'IGNORE_UPSTREAM' in job_config.keys():
            if job_config['IGNORE_UPSTREAM'] == "True":
                log.info("Bypassing upstream check as replay_config contains IGNORE_UPSTREAM=True")
                return True
        try:
            upstream_schedule ,  scheduler_state = self.get_upstream_job_information()
            bypassDisabledJobs = bool(Variable.get("bypass_disabled_jobs", default_var=True))
            log.info("Variable  bypass_disabled_jobs: '{}'".format(bypassDisabledJobs))
            if bypassDisabledJobs:
                if scheduler_state == 'disabled':
                    log.info("Bypassing upstream check as upstream job state is paused in scheduler")
                    return True

        except Exception as e:
            self.log.warning("error while fetching upstream schedule :: {}".format(e))
            context[SCHEDULER_ERR_MSG] = "error while fetching upstream schedule :: {}".format(e)
            return False

        last_upstream_schedule_time, _ = self.get_last_upstream_times(
            schedule_time, upstream_schedule)

        # get schedule window
        task_window = JobSpecTaskWindow(self._optimus_client, self.project_name, self.job_name)
        schedule_time_window_start, schedule_time_window_end = task_window.get_schedule_window(
            last_upstream_schedule_time.strftime(TIMESTAMP_FORMAT), upstream_schedule)

        self.log.info("waiting for upstream runs between: {} - {} schedule times of airflow dag run".format(
            schedule_time_window_start, schedule_time_window_end))

        if not self._are_all_job_runs_successful(schedule_time_window_start, schedule_time_window_end):
            self.log.warning("unable to find enough successful executions for upstream '{}' in "
                             "'{}' dates between {} and {}(inclusive), rescheduling sensor".
                             format(self.upstream_optimus_job, self.upstream_optimus_project, schedule_time_window_start,
                                    schedule_time_window_end))
            return False
        return True

    def get_last_upstream_times(self, schedule_time_of_current_job, upstream_schedule_interval):
        second_ahead_of_schedule_time = schedule_time_of_current_job + timedelta(seconds=1)
        c = croniter(upstream_schedule_interval, second_ahead_of_schedule_time)
        last_upstream_schedule_time = c.get_prev(datetime)
        last_upstream_execution_date = c.get_prev(datetime)
        return last_upstream_schedule_time, last_upstream_execution_date

    def get_upstream_job_information(self):
        job_metadata = self._upstream_optimus_client.get_job_metadata(self.upstream_optimus_namespace,
                                                             self.upstream_optimus_project, self.upstream_optimus_job)
        upstream_schedule = lookup_non_standard_cron_expression(job_metadata['spec']['interval'])
        return upstream_schedule, job_metadata['spec']['schedulerState']

    # TODO the api will be updated with getJobRuns even though the field here refers to scheduledAt
    #  it points to execution_date
    def _are_all_job_runs_successful(self, schedule_time_window_start, schedule_time_window_end) -> bool:
        try:
            api_response = self._upstream_optimus_client.get_job_run(
                self.upstream_optimus_project, self.upstream_optimus_job,
                schedule_time_window_start, schedule_time_window_end,
                self.project_name, self.job_name)
            self.log.info("job_run api response :: {}".format(api_response))
        except Exception as e:
            self.log.warning("error while fetching job runs :: {}".format(e))
            raise AirflowException(e)
        if api_response['message'] != "":
            self.log.info(api_response['message'])
        for job_run in api_response['jobRuns']:
            if job_run['state'] != 'success':
                self.log.info("failed for run :: {}".format(job_run))
                return False
        return True

    def _parse_date(self, timestamp):
        return datetime.strptime(timestamp, DATE_FORMAT).replace(tzinfo=timezone.utc)

    def _parse_datetime(self, timestamp) -> datetime:
        try:
            return datetime.strptime(timestamp, TIMESTAMP_FORMAT)
        except ValueError:
            return datetime.strptime(timestamp, TIMESTAMP_MS_FORMAT)


def optimus_notify(context, event_meta):
    params = context.get("params")
    optimus_client = OptimusAPIClient(params["optimus_hostname"])

    current_dag_id = context.get('task_instance').dag_id
    current_execution_date = context.get('execution_date')
    current_schedule_date = get_scheduled_at(context)

    # failure message pushed by failed tasks
    failure_messages = []

    def _xcom_value_has_error(_xcom) -> bool:
        return _xcom.key == XCOM_RETURN_KEY and isinstance(_xcom.value, dict) and 'error' in _xcom.value and \
               _xcom.value['error'] is not None

    for xcom in XCom.get_many(
            current_execution_date,
            key=None,
            task_ids=None,
            dag_ids=current_dag_id,
            include_prior_dates=False,
            limit=10):
        if xcom.key == 'error':
            failure_messages.append(xcom.value)
        if _xcom_value_has_error(xcom):
            failure_messages.append(xcom.value['error'])
    failure_message = ", ".join(failure_messages)

    if SCHEDULER_ERR_MSG in event_meta.keys():
        failure_message = failure_message + ", " + event_meta[SCHEDULER_ERR_MSG]
    if len(failure_message)>0:
        log.info(f'failures: {failure_message}')

    task_instance = context.get('task_instance')

    if event_meta["event_type"] == "TYPE_FAILURE" :
        dag_run = context['dag_run']
        tis = dag_run.get_task_instances()
        for ti in tis:
            if ti.state == TaskInstanceState.FAILED:
                task_instance = ti
                break

    message = {
        "log_url": task_instance.log_url,
        "task_id": task_instance.task_id,
        "attempt": task_instance.try_number,
        "duration"  : str(task_instance.duration),
        "exception" : str(context.get('exception')) or "",
        "message"   : failure_message,
        "scheduled_at"  : current_schedule_date.strftime(TIMESTAMP_FORMAT),
        "event_time"    : datetime.now().timestamp(),
    }
    message.update(event_meta)

    event = {
        "type": event_meta["event_type"],
        "value": message,
    }
    # post event
    log.info(event)
    resp = optimus_client.notify_event(params["project_name"], params["namespace"], params["job_name"], event)
    log.info(f'posted event {params}, {event}, {resp} ')
    return

def get_run_type(context):
    task_identifier = context.get('task_instance_key_str')
    try:
        job_name = context.get('params')['job_name']
        if task_identifier.split(job_name)[1].startswith("__wait_"):
            return "SENSOR"
        elif task_identifier.split(job_name)[1].startswith("__hook_"):
            return "HOOK"
        else:
            return "TASK"
    except Exception as e:
        return task_identifier


# job level events
def job_success_event(context):
    try:
        meta = {
            "event_context": EventContext.from_ctx(context, JOB_SUCCESS_EVENT).to_dict(),
            "event_type": "TYPE_JOB_SUCCESS",
            "status": "success"
        }
        result_for_monitoring = get_result_for_monitoring_from_xcom(context)
        if result_for_monitoring is not None:
            meta['monitoring'] = result_for_monitoring

        optimus_notify(context, meta)
    except Exception as e:
        print(e)

def job_failure_event(context):
    try:
        meta = {
            "event_context": EventContext.from_ctx(context, JOB_FAIL_EVENT).to_dict(),
            "event_type": "TYPE_FAILURE",
            "status": "failed"
        }
        result_for_monitoring = get_result_for_monitoring_from_xcom(context)
        if result_for_monitoring is not None:
            meta['monitoring'] = result_for_monitoring

        optimus_notify(context, meta)
    except Exception as e:
        print(e)

@dataclass
class EventContext:
    @dataclass
    class TaskInstance:
        max_tries: int
        task_id: str
        task_instance_key_str: str
        attempt: int
        log_url: str
        state: str
        start_date: str = None
        end_date: str = None

    @dataclass
    class DagRun:
        dag_id: str
        scheduled_at: str
        execution_date: str
        run_id: str
        start_date: str = None
        end_date: str = None

    @dataclass
    class Task:
        downstream_task_ids: list

    task_instance: TaskInstance
    dag_run: DagRun
    task: Task
    operator_type: str
    event_type: str
    event_reason: str

    @staticmethod
    def format_dt(dt):
        return dt.strftime(TIMESTAMP_FORMAT) if dt else None

    @classmethod
    def from_ctx(cls, ctx, event_type: str):
        ti = ctx.get("task_instance")
        dag_run = ctx.get("dag_run")
        current_execution_date = ctx.get("execution_date")
        current_schedule_date = get_scheduled_at(ctx)

        return cls(
            task_instance=cls.TaskInstance(
                max_tries=ti.max_tries,
                task_id=ti.task_id,
                task_instance_key_str = ctx.get("task_instance_key_str"),
                attempt=ti.try_number,
                log_url=ti.log_url,
                state=ti.state,
                start_date=cls.format_dt(ti.start_date),
                end_date=cls.format_dt(ti.end_date),
            ),
            dag_run=cls.DagRun(
                dag_id=ti.dag_id,
                scheduled_at=current_schedule_date.strftime(TIMESTAMP_FORMAT),
                execution_date=current_execution_date.strftime(TIMESTAMP_FORMAT),
                run_id=dag_run.run_id,
                start_date=cls.format_dt(dag_run.start_date),
                end_date=cls.format_dt(dag_run.end_date),
            ),
            task=cls.Task(
                downstream_task_ids=list(ti.task.downstream_task_ids),
            ),
            operator_type=get_run_type(ctx),
            event_type=event_type,
            event_reason = ctx.get("reason") if ctx.get("reason") != None else "",
        )

    def to_dict(self):
        return asdict(self)

# task level events
def operator_start_event(context):
    try:
        run_type = get_run_type(context)
        if run_type == "SENSOR":
            if not shouldSendSensorStartEvent(context):
                return
        meta = {
            "event_context": EventContext.from_ctx(context , OPERATOR_START_EVENT).to_dict(),
            "event_type": "TYPE_{}_START".format(run_type),
            "status": "running"
        }
        optimus_notify(context, meta)
    except Exception as e:
        print(e)

def operator_success_event(context):
    try:
        run_type = get_run_type(context)
        meta = {
            "event_context": EventContext.from_ctx(context, OPERATOR_SUCCESS_EVENT).to_dict(),
            "event_type": "TYPE_{}_SUCCESS".format(run_type),
            "status": "success"
        }
        optimus_notify(context, meta)
    except Exception as e:
        print(e)


def operator_retry_event(context):
    try:
        run_type = get_run_type(context)
        meta = {
            "event_context": EventContext.from_ctx(context, OPERATOR_RETRY_EVENT).to_dict(),
            "event_type": "TYPE_{}_RETRY".format(run_type),
            "status": "retried"
        }
        optimus_notify(context, meta)
    except Exception as e:
        print(e)


def operator_failure_event(context):
    try:
        run_type = get_run_type(context)
        meta = {
            "event_context": EventContext.from_ctx(context, OPERATOR_FAIL_EVENT).to_dict(),
            "event_type": "TYPE_{}_FAIL".format(run_type),
            "status": "failed"
        }
        if SCHEDULER_ERR_MSG in context.keys():
            meta[SCHEDULER_ERR_MSG] = context[SCHEDULER_ERR_MSG]

        optimus_notify(context, meta)
    except Exception as e:
        print(e)


def optimus_sla_miss_notify(dag, task_list, blocking_task_list, slas, blocking_tis):
    try:
        params = dag.params
        optimus_client = OptimusAPIClient(params["optimus_hostname"])

        slamiss_alert = int(Variable.get("slamiss_alert", default_var=1))
        if slamiss_alert != 1:
            return "suppressed slamiss alert"

        sla_list = []
        for sla in slas:
            sla_list.append({
                'task_id': sla.task_id,
                'dag_id': sla.dag_id,
                'scheduled_at' : dag.following_schedule(sla.execution_date).strftime(TIMESTAMP_FORMAT),
                'airflow_execution_time': sla.execution_date.strftime(TIMESTAMP_FORMAT),
                'timestamp': sla.timestamp.strftime(TIMESTAMP_FORMAT)
            })

        current_dag_id = dag.dag_id
        webserver_url = conf.get(section='webserver', key='base_url')
        message = {
            "slas": sla_list,
            "job_url": "{}/tree?dag_id={}".format(webserver_url, current_dag_id),
        }

        event = {
            "type": "TYPE_SLA_MISS",
            "value": message,
        }
        # post event
        resp = optimus_client.notify_event(params["project_name"], params["namespace"], params["job_name"], event)
        log.info(f'posted event {params}, {event}, {resp}')
        return
    except Exception as e:
        print(e)

def shouldSendSensorStartEvent(ctx):
    try:
        ti=ctx['ti']
        task_reschedules = TaskReschedule.find_for_task_instance(ti)
        if len(task_reschedules) == 0 :
            log.info(f'sending NEW sensor start event for attempt number-> {ti.try_number}')
            return True
        log.info("ignoring sending sensor start event as its not first poke")
    except Exception as e:
        print(e)

def get_result_for_monitoring_from_xcom(ctx):
    try:
        ti = ctx.get('task_instance')
        return_value = ti.xcom_pull(key='return_value')
    except Exception as e:
        log.info(f'error getting result for monitoring: {e}')

    if type(return_value) is dict:
        if 'monitoring' in return_value:
            return return_value['monitoring']
    return None

# everything below this is here for legacy reasons, should be cleaned up in future

def alert_failed_to_slack(context):
    SLACK_CONN_ID = "slack_alert"
    TASKFAIL_ALERT = int(Variable.get("taskfail_alert", default_var=1))
    SLACK_CHANNEL = Variable.get("slack_channel")

    def _xcom_value_has_error(_xcom) -> bool:
        return _xcom.key == XCOM_RETURN_KEY and isinstance(_xcom.value, dict) and 'error' in _xcom.value and \
               _xcom.value['error'] != None

    if TASKFAIL_ALERT != 1:
        return "suppressed failure alert"

    slack_token = ""
    try:
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    except:
        print("no slack connection variable set")
        return "{connection} connection variable not defined, unable to send alerts".format(connection=SLACK_CONN_ID)

    if not SLACK_CHANNEL:
        return "no slack channel variable set"

    current_dag_id = context.get('task_instance').dag_id
    current_task_id = context.get('task_instance').task_id
    current_execution_date = context.get('execution_date')

    # failure message pushed by failed tasks
    failure_messages = []
    for xcom in XCom.get_many(
            current_execution_date,
            key=None,
            task_ids=None,
            dag_ids=current_dag_id,
            include_prior_dates=False,
            limit=10):
        if xcom.key == 'error':
            failure_messages.append(xcom.value)
        if _xcom_value_has_error(xcom):
            failure_messages.append(xcom.value['error'])
    failure_message = ", ".join(failure_messages)
    if failure_message != "":
        log.info(f'failures: {failure_message}')

    message_body = "\n".join([
        "• *DAG*: {}".format(current_dag_id),
        "• *Task*: {}".format(current_task_id),
        "• *Execution Time*: {}".format(current_execution_date),
        "• *Run ID*: {}".format(context.get('run_id'))
    ])

    message_footer = "\n".join([
        ":blob-facepalm: Owner: {}".format(context.get('dag').owner),
        ":hourglass: Duration: {} sec".format(context.get('task_instance').duration),
        ":memo: Details: {}".format(failure_message)
    ])

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Task failed :fire:"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message_body
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "danger",
                    "text": {
                        "type": "plain_text",
                        "text": "View log :airflow:",
                    },
                    "url": context.get('task_instance').log_url,
                    "action_id": "view_log",
                }
            ]
        },
        {
            "type": "divider"
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": message_footer
                },
            ]
        },
    ]
    failed_alert = SlackAPIPostOperator(
        slack_conn_id=SLACK_CONN_ID,
        token=slack_token,
        blocks=blocks,
        task_id='slack_failed_alert',
        channel=SLACK_CHANNEL
    )
    return failed_alert.execute(context=context)


class ExternalHttpSensor(BaseSensorOperator):
    """
    Executes a HTTP GET statement and returns False on failure caused by
    404 Not Found

    :param method: The HTTP request method to use
    :param endpoint: The relative part of the full url
    :param request_params: The parameters to be added to the GET url
    :param headers: The HTTP headers to be added to the GET request

    """

    template_fields = ('endpoint', 'request_params', 'headers')

    def __init__(
            self,
            endpoint: str,
            method: str = 'GET',
            request_params: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, Any]] = None,
            *args,
            **kwargs,
    ) -> None:
        kwargs['mode'] = kwargs.get('mode', 'reschedule')
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.request_params = request_params or {}
        self.headers = headers or {}

    def poke(self, context: 'Context') -> bool:
        self.log.info('Poking: %s', self.endpoint)
        r = requests.get(url=self.endpoint, headers=self.headers, params=self.request_params, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        if (r.status_code >= 200 and r.status_code <= 300):
            return True
        return False
