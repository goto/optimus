package service

import (
	"context"

	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
)

type ChangelogRepository interface {
	GetChangelogs(ctx context.Context, projectName tenant.ProjectName, resourceName resource.Name) ([]*resource.ChangeLog, error)
}

// right now this is done to capture the feature adoption
var getChangelogFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "get_resource_changelog_errors",
	Help: "errors occurred in get resource changelog",
}, []string{"project", "resource", "error"})

type ChangelogService struct {
	repo   ChangelogRepository
	logger log.Logger
}

func NewChangelogService(logger log.Logger, repo ChangelogRepository) *ChangelogService {
	return &ChangelogService{
		repo:   repo,
		logger: logger,
	}
}

func (cs ChangelogService) GetChangelogs(ctx context.Context, projectName tenant.ProjectName, resourceName resource.Name) ([]*resource.ChangeLog, error) {
	changelogs, err := cs.repo.GetChangelogs(ctx, projectName, resourceName)
	if err != nil {
		cs.logger.Error("error getting changelog for resource [%s]: %s", resourceName.String(), err)
		getChangelogFailures.WithLabelValues(
			projectName.String(),
			resourceName.String(),
			err.Error(),
		).Inc()
	}

	return changelogs, err
}
