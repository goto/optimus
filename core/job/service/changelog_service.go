package service

import (
	"context"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
)

type ChangeLogService struct {
	jobRepo JobRepository
}

func (cl *ChangeLogService) GetChangelog(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.ChangeLog, error) {
	changelog, err := cl.jobRepo.GetChangelog(ctx, projectName, jobName)
	if err != nil {
		getChangelogFailures.WithLabelValues(
			projectName.String(),
			jobName.String(),
			err.Error(),
		).Inc()
	}
	getChangelogFeatureAdoption.WithLabelValues(
		projectName.String(),
		jobName.String(),
	).Inc()

	return changelog, err
}

func NewChangeLogService(jobRepo JobRepository) *ChangeLogService {
	return &ChangeLogService{
		jobRepo: jobRepo,
	}
}
