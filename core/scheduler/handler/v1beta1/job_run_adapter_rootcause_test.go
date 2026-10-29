package v1beta1 //nolint:testpackage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

func TestFromRootCauseRequest(t *testing.T) {
	referenceTime := time.Date(2026, 10, 26, 1, 30, 0, 0, time.UTC)

	t.Run("adapts the project-level filters into the service request", func(t *testing.T) {
		req := &pb.IdentifySLABreachRootCauseRequest{
			ProjectNames: []string{"p_root"},
			JobNames:     []string{"job_target"},
			JobLabels: map[string]*pb.LabelValues{
				"team": {Values: []string{"de-1", "de-2"}},
			},
			ReferenceTime:         timestamppb.New(referenceTime),
			EnableAlert:           true,
			ScheduledRangeInHours: 2,
		}

		reqConfig, err := fromRootCauseRequest(req)

		assert.NoError(t, err)
		assert.Len(t, reqConfig.ProjectNames, 1)
		assert.Equal(t, tenant.ProjectName("p_root"), reqConfig.ProjectNames[0])
		assert.Equal(t, []scheduler.JobName{"job_target"}, reqConfig.JobNames)
		assert.Equal(t, map[string][]string{"team": {"de-1", "de-2"}}, reqConfig.Labels)
		assert.Equal(t, referenceTime, reqConfig.ReferenceTime)
		assert.True(t, reqConfig.EnableAlert)
		assert.Equal(t, 2*time.Hour, reqConfig.ScheduleRangeInHours)
	})

	t.Run("rejects an empty project name", func(t *testing.T) {
		req := &pb.IdentifySLABreachRootCauseRequest{ProjectNames: []string{""}}

		_, err := fromRootCauseRequest(req)

		assert.Error(t, err)
	})
}

func TestToRootCauseResponse(t *testing.T) {
	scheduledAt := time.Date(2026, 10, 26, 1, 0, 0, 0, time.UTC)
	incidents := []*scheduler.SLABreachIncidentWithImpacts{
		{
			Incident: &scheduler.SLABreachIncident{
				Key: scheduler.SLABreachIncidentKey{
					ProjectName: tenant.ProjectName("p_root"),
					JobName:     "job_offender",
					ScheduledAt: scheduledAt,
				},
				Details: scheduler.RootCauseDetails{
					Reasons: []scheduler.RootCauseReasonDetail{
						{
							Reason:              scheduler.TierRuntimeDelay,
							Status:              scheduler.ReasonStatusActive,
							InducedDelaySeconds: 2400,
						},
						{
							Reason:              scheduler.TierThirdPartyDelay,
							Status:              scheduler.ReasonStatusResolved,
							InducedDelaySeconds: 600,
							BlockedOnSensors:    []string{"wait_dex_events"},
							SourceType:          "dex",
						},
					},
				},
				MaxInducedDelaySeconds: 2400,
			},
			Impacts: []scheduler.ImpactedJob{
				{ProjectName: "p_down", JobName: "job_down", ScheduledAt: scheduledAt.Add(time.Hour)},
			},
		},
	}

	response := toRootCauseResponse(incidents)

	assert.Len(t, response.RootCauses, 1)
	rootCause := response.RootCauses[0]
	assert.Equal(t, "p_root", rootCause.ProjectName)
	assert.Equal(t, "job_offender", rootCause.JobName)
	assert.Equal(t, timestamppb.New(scheduledAt), rootCause.ScheduledAt)
	assert.Equal(t, durationpb.New(40*time.Minute), rootCause.MaxInducedDelay)

	assert.Len(t, rootCause.Reasons, 2)
	assert.Equal(t, pb.RootCauseReason_ROOT_CAUSE_REASON_RUNTIME_DELAY, rootCause.Reasons[0].Reason)
	assert.Equal(t, pb.RootCauseStatus_ROOT_CAUSE_STATUS_ACTIVE, rootCause.Reasons[0].Status)
	assert.Equal(t, durationpb.New(40*time.Minute), rootCause.Reasons[0].InducedDelay)
	assert.Equal(t, pb.RootCauseReason_ROOT_CAUSE_REASON_THIRD_PARTY_DELAY, rootCause.Reasons[1].Reason)
	assert.Equal(t, pb.RootCauseStatus_ROOT_CAUSE_STATUS_RESOLVED, rootCause.Reasons[1].Status)
	assert.Equal(t, []string{"wait_dex_events"}, rootCause.Reasons[1].BlockedOnSensors)
	assert.Equal(t, "dex", rootCause.Reasons[1].SourceType)

	assert.Len(t, rootCause.ImpactedJobs, 1)
	assert.Equal(t, "p_down", rootCause.ImpactedJobs[0].ProjectName)
	assert.Equal(t, "job_down", rootCause.ImpactedJobs[0].JobName)

	// an empty evaluation yields an empty list, not nil
	assert.NotNil(t, toRootCauseResponse(nil).RootCauses)
}
