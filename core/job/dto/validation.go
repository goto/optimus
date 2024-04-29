package dto

import (
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
)

type ValidateRequest struct {
	Tenant       tenant.Tenant
	JobSpecs     []*job.Spec
	JobNames     []string
	DeletionMode bool
}

type ValidateResult struct {
	Stage    ValidateStage
	Messages []string
	Success  bool
}
