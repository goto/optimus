package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
)

type mockSubscriberIncidentRepo struct {
	mock.Mock
}

func (m *mockSubscriberIncidentRepo) UpsertIncident(ctx context.Context, incident *scheduler.SLABreachIncident, impactedJobs []scheduler.ImpactedJob) error {
	return m.Called(ctx, incident, impactedJobs).Error(0)
}

func (m *mockSubscriberIncidentRepo) GetActiveIncidents(ctx context.Context, since time.Time) ([]*scheduler.SLABreachIncidentWithImpacts, error) {
	args := m.Called(ctx, since)
	if incidents, ok := args.Get(0).([]*scheduler.SLABreachIncidentWithImpacts); ok {
		return incidents, args.Error(1)
	}
	return nil, args.Error(1)
}

func TestIdentifySLABreachesBatch_SubscriberMode(t *testing.T) {
	ctx := context.Background()
	l := log.NewNoop()

	impactedProject := tenant.ProjectName("p_impacted")
	offenderProject := tenant.ProjectName("p_offender")
	impactedTnnt, _ := tenant.NewTenant("p_impacted", "team-impacted")
	projectEntity, _ := tenant.NewProject("p_impacted", map[string]string{
		"STORAGE_PATH":                 "file:///tmp/",
		"SCHEDULER_HOST":               "http://localhost",
		tenant.ProjectAlertManagerTeam: "impacted-team",
	}, map[string]string{})
	namespaceEntity, _ := tenant.NewNamespace("team-impacted", impactedProject, map[string]string{}, map[string]string{})
	tenantWithDetails, _ := tenant.NewTenantDetails(projectEntity, namespaceEntity, tenant.PlainTextSecrets{})

	rootCauseScheduledAt := time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC)
	impactedScheduledAt := rootCauseScheduledAt.Add(time.Hour)

	incident := &scheduler.SLABreachIncidentWithImpacts{
		Incident: &scheduler.SLABreachIncident{
			Key: scheduler.SLABreachIncidentKey{
				ProjectName: offenderProject,
				JobName:     "job_offender",
				ScheduledAt: rootCauseScheduledAt,
			},
			Details: scheduler.RootCauseDetails{
				Reasons: []scheduler.RootCauseReasonDetail{
					{Reason: scheduler.TierRuntimeDelay, Status: scheduler.ReasonStatusActive, InducedDelaySeconds: 3600},
					{Reason: scheduler.TierFailedUpstream, Status: scheduler.ReasonStatusActive},
				},
			},
		},
		Impacts: []scheduler.ImpactedJob{
			{ProjectName: impactedProject, JobName: "job_impacted", ScheduledAt: impactedScheduledAt},
			{ProjectName: impactedProject, JobName: "job_skipped", ScheduledAt: impactedScheduledAt},
		},
	}

	impactedJob := &scheduler.JobWithDetails{
		Name: "job_impacted",
		Job: &scheduler.Job{
			Tenant: impactedTnnt,
			Name:   "job_impacted",
		},
		Schedule: &scheduler.Schedule{
			StartDate: rootCauseScheduledAt.Add(-48 * time.Hour),
			Interval:  "0 * * * *",
		},
		Alerts: []scheduler.Alert{{On: scheduler.EventCategorySLAMiss, Config: map[string]string{"duration": "1h"}}},
	}

	t.Run("alerts the impacted team from persisted incidents without lineage computation", func(t *testing.T) {
		incidentRepo := &mockSubscriberIncidentRepo{}
		incidentRepo.On("GetActiveIncidents", ctx, mock.Anything).Return([]*scheduler.SLABreachIncidentWithImpacts{incident}, nil).Once()

		detailsGetter := NewJobDetailsGetter(t)
		detailsGetter.On("GetJobs", ctx, impactedProject, []string{"job_impacted"}).Return([]*scheduler.JobWithDetails{impactedJob}, nil).Once()

		// no lineage fetch expectations: subscriber mode must not walk the lineage
		lineageFetcher := NewJobLineageFetcher(t)

		tenantGetter := new(TenantGetter)
		tenantGetter.On("GetDetails", ctx, impactedTnnt).Return(tenantWithDetails, nil).Once()

		notifier := &mockPotentialSLANotifier{}
		// one alert per active reason, both routed to the impacted team
		notifier.On("SendPotentialSLABreach", mock.Anything).Twice().Run(func(args mock.Arguments) {
			alert := args.Get(0).(*scheduler.PotentialSLABreachAlert)
			assert.Equal(t, "impacted-team", alert.Team)
			assert.Equal(t, "p_impacted", alert.Project)
			assert.Equal(t, "job_offender", alert.RootCauseJob)
			assert.Equal(t, "p_offender", alert.RootCauseProject)
			assert.Equal(t, rootCauseScheduledAt, *alert.RootCauseScheduledAt)
		})

		svc := service.NewJobSLAPredictorService(l, config.PotentialSLABreachConfig{SubscriberMode: true}, config.RootCauseConfig{},
			nil, lineageFetcher, NewDurationEstimator(t), detailsGetter, notifier, tenantGetter,
			NewScheduledChangeGetter(t), nil, nil, nil, incidentRepo)

		response, err := svc.IdentifySLABreachesBatch(ctx, []scheduler.SLABreachCombo{{ProjectName: impactedProject}},
			service.JobSLAPredictorRequestConfig{
				ReferenceTime: rootCauseScheduledAt.Add(2 * time.Hour),
				EnableAlert:   true,
				Severity:      "WARNING",
				SkipJobNames:  []string{"job_skipped"},
			})

		assert.NoError(t, err)
		assert.Len(t, response, 1)
		breach := response["p_impacted/job_impacted"]
		assert.NotNil(t, breach)
		assert.Equal(t, "p_impacted", breach.TargetProject)
		assert.Len(t, breach.Upstreams, 1)
		// the response carries the impacted run's own SLA deadline
		assert.Equal(t, impactedScheduledAt.Add(time.Hour), *breach.Upstreams["job_offender"].InferredSLA)

		lineageFetcher.AssertNotCalled(t, "GetJobLineage")
		notifier.AssertExpectations(t)
		detailsGetter.AssertExpectations(t)
	})

	t.Run("subscriber mode without an incident repository errors", func(t *testing.T) {
		svc := service.NewJobSLAPredictorService(l, config.PotentialSLABreachConfig{SubscriberMode: true}, config.RootCauseConfig{},
			nil, NewJobLineageFetcher(t), NewDurationEstimator(t), NewJobDetailsGetter(t), &mockPotentialSLANotifier{},
			new(TenantGetter), NewScheduledChangeGetter(t), nil, nil, nil, nil)

		_, err := svc.IdentifySLABreachesBatch(ctx, nil, service.JobSLAPredictorRequestConfig{
			ReferenceTime: time.Now().UTC(),
		})

		assert.Error(t, err)
	})
}
