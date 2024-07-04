package service

import (
	"context"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ChangelogRepository interface {
	GetChangelogs(ctx context.Context, projectName tenant.ProjectName, resourceName resource.Name) ([]*resource.ChangeLog, error)
}

var (
	// right now this is done to capture the feature adoption
	getChangelogFeatureAdoption = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "get_changelog_total",
		Help: "number of requests received for viewing changelog",
	}, []string{"project", "resource", "type"})

	getChangelogFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "get_changelog_errors",
		Help: "errors occurred in get changelog",
	}, []string{"project", "resource", "type", "error"})
)

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
	var err error

	defer func() {
		if err != nil {
			getChangelogFailures.WithLabelValues(
				projectName.String(),
				resourceName.String(),
				resource.EntityResource,
				err.Error(),
			).Inc()

			return
		}

		getChangelogFeatureAdoption.WithLabelValues(
			projectName.String(),
			resourceName.String(),
			resource.EntityResource,
		).Inc()
	}()

	changelogs, err := cs.repo.GetChangelogs(ctx, projectName, resourceName)
	if err != nil {
		cs.logger.Error("error getting changelog for resource [%s]: %s", resourceName.String(), err)
		return nil, err
	}

	return changelogs, err
}
