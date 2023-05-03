package event

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	pbInt "github.com/goto/optimus/protos/gotocompany/optimus/integration/v1beta1"
)

type JobRunStateChanged struct {
	Event

	JobRun *scheduler.JobRun
}

func NewJobRunStateChangeEvent(jobRun *scheduler.JobRun) (*JobRunStateChanged, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobRunStateChanged{
		Event:  baseEvent,
		JobRun: jobRun,
	}, nil
}

func (j *JobRunStateChanged) Bytes() ([]byte, error) {
	occurredAt := timestamppb.New(j.Event.OccurredAt)
	var eventType pbInt.OptimusChangeEvent_EventType
	switch j.JobRun.State {
	case scheduler.StateWaitUpstream:
		eventType = pbInt.OptimusChangeEvent_JOB_WAIT_UPSTREAM
	case scheduler.StateInProgress:
		eventType = pbInt.OptimusChangeEvent_JOB_IN_PROGRESS
	case scheduler.StateSuccess:
		eventType = pbInt.OptimusChangeEvent_JOB_SUCCESS
	case scheduler.StateFailed:
		eventType = pbInt.OptimusChangeEvent_JOB_FAILURE
	}

	optEvent := &pbInt.OptimusChangeEvent{
		EventId:       j.Event.ID.String(),
		OccurredAt:    occurredAt,
		ProjectName:   j.JobRun.Tenant.ProjectName().String(),
		NamespaceName: j.JobRun.Tenant.NamespaceName().String(),
		EventType:     eventType,
		Payload: &pbInt.OptimusChangeEvent_JobRun{
			JobRun: &pbInt.JobRunPayload{
				JobName:     j.JobRun.JobName.String(),
				ScheduledAt: timestamppb.New(j.JobRun.ScheduledAt),
				JobRunId:    j.JobRun.ID.String(),
			},
		},
	}
	return proto.Marshal(optEvent)
}
