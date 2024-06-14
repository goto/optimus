CREATE ROLE MONITORING;

GRANT CONNECT ON DATABASE OPTIMUS TO MONITORING;

GRANT SELECT ON JOB_RUN , SENSOR_RUN , TASK_RUN , HOOK_RUN , JOB_UPSTREAM, JOB , RESOURCE, PROJECT, NAMESPACE, REPLAY_REQUEST, REPLAY_RUN TO MONITORING;

CREATE USER MONITORING_USER WITH PASSWORD '*********';

GRANT MONITORING TO MONITORING_USER;