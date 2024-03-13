# Webhooks

## Overview:
For Data Pipelines, there is often a requirement that systems external to Optimus expect notification/nudge upon job run state change. This helps people trigger external routines.

## Scope:
Only HTTP/HTTPS webhooks are supported.
### On Events:
| Event           | Optimus Event Category |
| --------------- | ---------------------- | 
| Job Fail        | 'failure'                | 
| Job Success     | 'success'                | 
| Job SLA Miss    | 'sla_miss'               | 


## WebHook-Request:
### Method:
> HTTP POST
### URL:
> User configured
### Request Content-Type:
> application/json
### Request Body:
```json
{
   "job"            : "string",
   "project"        : "string",
   "namespace"      : "string",
   "destination"    : "string",
   "scheduled_at"   : "time-string:UTC",
   "status"         : "string",
   "job_label": {
       "key"    : "value",
       "key1"   : "value",
       "key2"   : "value"
   }
}

```

### Expected Response
- 200

### On Error
- Log it in Optimus Server

### Retry Strategy
- None

## Job Specification to Take into Account WebHook URLs
```yaml
version: 1
name: <JOB_NAME>
owner: <OWNER>
schedule:
  start_date: "2021-09-01"
  interval: 0 5 * * *
behavior:
  webhook:
    - on: success
      endpoints:
        - url: http://sub-domain.domain.com/path/to/the/webhook?some=somethingStatic
          headers:
            auth-header: '{{.secret.WEBHOOK_SECRET}}'
            some_header: 'dummy value'
    - on: sla_miss
      endpoints:
        - url: http://sub-domain.domain.com/path/to/the/webhook?some=somethingStatic
    - on: failure
      endpoints:
        - url: http://sub-domain.domain.com/path/to/the/webhook1
          headers:
            auth: 'bearer: {{.secret.WEBHOOK_SECRET}}'
notify:
  - on: failure
    channels:
      - slack://#somechannel
      - pagerduty://#somechannel
task:
  name: <task_name>
  config:
    labels:
      label_name: <some_label>
dependencies: []
