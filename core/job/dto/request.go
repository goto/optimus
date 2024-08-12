package dto

import (
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
)

type JobToDeleteRequest struct {
	Namespace tenant.NamespaceName
	JobName   job.Name
}

type BulkDeleteTracker struct {
	JobName string
	Message string
	Success bool
}
