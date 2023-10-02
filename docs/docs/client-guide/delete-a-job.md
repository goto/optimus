# Delete a Job

Some time, there is a need to delete an existing job, like when the job is not needed anymore but one or more of downstreams still require it. For such reason, Optimus provides a mechanism to delete job from client.

To delete an existing job, run the following command:

```shell
$ optimus job delete {job_name} [flags]
```

This command requires Optimus client config to be in the same directory with the Optimus, or it can be specified through `--config` flag. The following is the available flags.

Flag | Required | Description | Example
--- | --- | --- | ---
-c, --config | `true` if client config is not found, `false` otherwise | File path for client configuration | ./optimus.yaml
--force | `false` | Whether to force delete regardless of downstream or not | true
--clean-history | `false` | Whether to clean hostory or not | true

_Note: confirmations will be prompted when this command is triggered_
