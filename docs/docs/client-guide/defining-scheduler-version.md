# Defining Scheduler Version
For now, optimus only supports airflow as a scheduler. Optimus provides capability to determine the scheduler version per project by defining `scheduler_version` in project config (`optimus.yaml`). By default, optimus use airflow version 2.1 if it is not specified in `optimus.yaml` config.

Optimus supports these following version:
| Version |
|---|
| 2.1 |
| 2.4 |
