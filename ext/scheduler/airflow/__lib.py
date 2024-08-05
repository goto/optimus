import json
import logging
import base64
import pendulum
import requests

from typing import Dict
from datetime import datetime, timedelta
from croniter import croniter
from kubernetes.client import models as k8s

from airflow.configuration import conf
from airflow.models import (Variable, XCom, TaskReschedule )
try:
    from airflow.models import XCOM_RETURN_KEY
except ImportError:
    from airflow.utils.xcom import XCOM_RETURN_KEY  # airflow >= 2.5
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import TaskInstanceState
from airflow.exceptions import AirflowException


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# UTC time zone as a tzinfo instance.
utc = pendulum.timezone('UTC')

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIMESTAMP_MS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

SCHEDULER_ERR_MSG = "scheduler_error"
STARTUP_TIMEOUT_IN_SECS = int(Variable.get("startup_timeout_in_secs", default_var=2 * 60))
OPTIMUS_REQUEST_TIMEOUT_IN_SECS = int(Variable.get("optimus_request_timeout_in_secs", default_var=5 * 60))

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
    job_cron_iter = croniter(context.get("dag").schedule_interval, context.get('execution_date'))
    return job_cron_iter.get_next(datetime)

class SuperKubernetesPodOperator(KubernetesPodOperator):
    def __init__(self, *args, **kwargs):
        kwargs["startup_timeout_seconds"] = STARTUP_TIMEOUT_IN_SECS
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

    def get_job_run(self, project_name: str, job_name: str, schedule_time: str, upstream_host: str,  upstream_project_name: str,  upstream_namespace_name: str, upstream_job_name: str) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{optimus_project}/job/{optimus_job}/upstream/run'.format(
            optimus_host=self.host,
            optimus_project=project_name,
            optimus_job=job_name,
        )
        # upstream_host base 64 encode
        upstream_host = base64.b64encode(upstream_host.encode(), altchars=None)

        response = requests.get(url, params={
            'schedule_time': schedule_time,
            'upstream_host': upstream_host.decode(),
            'upstream_project_name': upstream_project_name,
            'upstream_namespace_name': upstream_namespace_name,
            'upstream_job_name': upstream_job_name
        }, timeout=OPTIMUS_REQUEST_TIMEOUT_IN_SECS)
        self._raise_error_if_request_failed(response)
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

    def poke(self, context):
        try:
            schedule_time = get_scheduled_at(context)
            upstream_host = ""
            if self.upstream_optimus_hostname != self.optimus_hostname:
                upstream_host = self.upstream_optimus_hostname

            api_response = self._optimus_client.get_job_run( self.project_name,
                                              self.job_name,
                                              schedule_time.strftime(TIMESTAMP_FORMAT),
                                              upstream_host,
                                              self.upstream_optimus_project,
                                              self.upstream_optimus_namespace,
                                              self.upstream_optimus_job)

        except Exception as e:
            self.log.warning("error while fetching job runs :: {}".format(e))
            raise AirflowException(e)

        self.log.info("upstream runs :: {}".format(api_response['jobRuns']))
        for job_run in api_response['jobRuns']:
            if job_run['state'] != 'success':
                self.log.info("failed for run :: {}".format(job_run))

        return api_response['allSuccess']


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
            "event_type": "TYPE_FAILURE",
            "status": "failed"
        }
        result_for_monitoring = get_result_for_monitoring_from_xcom(context)
        if result_for_monitoring is not None:
            meta['monitoring'] = result_for_monitoring

        optimus_notify(context, meta)
    except Exception as e:
        print(e)


# task level events
def operator_start_event(context):
    try:
        run_type = get_run_type(context)
        if run_type == "SENSOR":
            if not shouldSendSensorStartEvent(context):
                return
        meta = {
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