package dto

import "github.com/goto/optimus/core/job"

type UpsertResult struct {
	JobName job.Name
	Status  job.DeployState
}
