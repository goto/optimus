# Intervals and Windows

When defining a new job, you need to define the **interval (cron)** at which it will be triggered. This parameter can give 
you a precise value when the job is scheduled for execution but only a rough estimate exactly when the job is executing. 
It is very common in a ETL pipeline to know when the job is exactly executing as well as for what time window the current 
transformation will consume the data.

For example, assume there is a job that querying from a table using below statement:
```sql
SELECT * FROM table WHERE
created_at >= DATE('{{.DSTART}}') AND
created_at < DATE('{{.DEND}}')
```

**_DSTART_** and **_DEND_** could be replaced at the time of compilation with based on its window configuration. 
Without the provided filter, we will have to consume all the records which are created till date inside the table 
even though the previous rows might already been processed.

These _DSTART_ and _DEND_ values of the input window could vary depending on the ETL job requirement.
- For a simple transformation job executing daily, it would need to consume full day work of yesterday’s data.
- A job might be consuming data for a week/month for an aggregation job, but the data boundaries should be complete, 
  not consuming any partial data of a day.

## Window Configuration

Optimus allows user to define the amount of data window to consume through window configurations. The configurations 
act on the schedule_time of the job and applied in order to compute _DSTART_ and _DEND_.

The following is the list of available confiugration the user can setup a window:

- **Truncate_to**: The data window on most of the scenarios needs to be aligned to a well-defined time window
  like month start to month end, or week start to weekend with week start being monday, or a complete day.
  Inorder to achieve that the truncate_to option is provided which can be configured with either of these values
  "h", "d", "w", "M" through which for a given schedule_time the end_time will be the end of last hour, day, week, month respectively.
- **Offset**: Offset is time duration configuration which enables user to move the `end_time` post truncation.
  User can define the duration like "24h", "2h45m", "60s", "-45m24h", "0", "", "2M", "45M24h", "45M24h30m"
  where "h","m","s","M" means hour, month, seconds, Month respectively.
- **Size**: Size enables user to define the amount of data to consume from the `end_time` again defined through the duration same as offset.

To further understand, the following is an example with its explanation. **Important** note, the following example uses
window `version: 2` because `version: 1` will soon be deprecated.

For example, previous-mentioned job has `0 2 * * *` schedule interval and is scheduled to run on 
**2023-03-07 at 02.00 UTC** with following details:

| Configuration | Value | Description                                                                            |
|---------------|-------|----------------------------------------------------------------------------------------|
| Truncate_to   | d     | Even though it is scheduled at 02.00 AM, data window will be day-truncated (00.00 AM). |
| Offset        | -24h  | Shifts the window to be 1 day earlier.                                                 |
| Size          | 24h   | Gap between DSTART and DEND is 24h.                                                    |

Above configuration will produce below window:
- _DSTART_: 2023-03-05T00:00:00Z
- _DEND_: 2023-03-06T00:00:00Z

This means, the query will be compiled to the following query

```sql
SELECT * FROM table WHERE
created_at >= DATE('2023-03-05T00:00:00Z') AND
created_at < DATE('2023-03-06T00:00:00Z')
```

Assume the table content is as the following:

| name    | created_at |
| ------- |------------|
| Rick    | 2023-03-05 |
| Sanchez | 2023-03-06 |
| Serious | 2023-03-07 |
| Sam     | 2023-03-07 |

When the job that scheduled at **2023-03-07** runs, the job will consume `Rick` as the input of the table.

Window configuration can be specified in two ways, through custom window configuration and through window preset.

### Custom Window

Through this option, the user can directly configure the window that meets their requirement in the job spec YAML.
The following is an example of its usage:

```yaml
version: 1 # decides window version
name: sample-project.playground.table1
owner: sample_owner
schedule:
  ...
behavior:
  ...
task:
  name: bq2bq
  config:
    ...
  window:
    size: 24h
    offset: "0"
    truncate_to: d
labels:
  ...
hooks: []
dependencies: []
```

Notice the window configuration is specified under field `task.window`. **Important** note, the `version` field decides which
version of window capability to be used. Currently available is window `version: 1` and window `version: 2`. Version 2 is recommended
to be used as verion 1 will soon be deprecated. To know the difference between the two version, run the `playground` feature for window:

```bash
optimus playground window
```

### Window Preset (since v0.10.0)

Window preset is a feature that allows easier setup of window configuration while also maintaining consistency. Through this feature,
the user can configure a definition of window once, then use it multiple times through the jobs which require it. **Important** note,
window preset always use window `version: 2`. The main components of window preset are as follow.

* **Window Preset File**

Presets configuration is put in a dedicated YAML file. The way to configure it still uses the same window configuration
like `truncate_to`, `offset`, and `size`. Though, there are some additions, like the name of the preset and the description to explain this preset.
The following is an example of how to define a preset under `presets.yaml` file (note that the file name does not have to be this one).

```yaml
presets:
  yesterday:
    description: defines yesterday window
    window:
      truncate_to: d
      offset: -24h
      size: 24h
  last_month:
    description: defines last 30 days window
    window:
      truncate_to: M
      offset: -30d
      size: 30d
```

In the above example, the file `presets.yaml` defines two presets, named `yesterday` and `last_month`. The name of preset **SHOULD** be
in lower case. All of the fields are required, unless specified otherwise.

* **Preset Reference under Project**

If the preset file is already specified, the next thing to do is to ensure that the preset file is referenced under project configuration.
The following is an example to refer the preset file under project configuration:

```yaml
version: 1
log:
  ...
host: localhost:9100
project:
  name: development_project
  preset_path: ./preset.yaml # points to preset file
  config:
    ...
namespaces:
  ...
```

In the above example, a new field is present, named `preset_path`. This path refers to where the preset file is located.

* **Preset Reference for Job Specification**

Now, if the other two components are met, where the window preset file is specified and this file is referenced by the project, it means
it is ready to be used. And the way to use it is by referencing which preset to be used in whichever job requires it. The following is an example
of its usage:

```yaml
version: 1 # preset always use window version 2
name: sample-project.playground.table1
owner: sample_owner
schedule:
  ...
behavior:
  ...
task:
  name: bq2bq
  config:
    ...
  window:
    preset: yesterday
labels:
  ...
hooks: []
dependencies: []
```

**Important** note, preset is optional in nature. It means that even if the preset is specified, the user can still use
the custom window configuration depending on their need.
