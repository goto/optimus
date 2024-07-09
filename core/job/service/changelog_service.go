package service

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
)

var getChangelogFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "get_changelog_errors",
	Help: "errors occurred in get changelog",
}, []string{"project", "job", "error"})

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

	return changelog, err
}

func NewChangeLogService(jobRepo JobRepository) *ChangeLogService {
	return &ChangeLogService{
		jobRepo: jobRepo,
	}
}
