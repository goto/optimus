package event

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	pbInt "github.com/goto/optimus/protos/gotocompany/optimus/integration/v1beta1"
)

type JobRunWaitUpstream struct {
	Event

	JobRun *scheduler.JobRun
}

func (j *JobRunWaitUpstream) Bytes() ([]byte, error) {
	return proto.Marshal(toOptimusChangeEvent(j.JobRun, j.Event))
}

type JobRunInProgress struct {
	Event

	JobRun *scheduler.JobRun
}

func (j *JobRunInProgress) Bytes() ([]byte, error) {
	return proto.Marshal(toOptimusChangeEvent(j.JobRun, j.Event))
}

type JobRunSuccess struct {
	Event

	JobRun *scheduler.JobRun
}

func (j *JobRunSuccess) Bytes() ([]byte, error) {
	return proto.Marshal(toOptimusChangeEvent(j.JobRun, j.Event))
}

type JobRunFailed struct {
	Event

	JobRun *scheduler.JobRun
}

func (j *JobRunFailed) Bytes() ([]byte, error) {
	return proto.Marshal(toOptimusChangeEvent(j.JobRun, j.Event))
}

func NewJobRunWaitUpstreamEvent(jobRun *scheduler.JobRun) (*JobRunWaitUpstream, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobRunWaitUpstream{
		Event:  baseEvent,
		JobRun: jobRun,
	}, nil
}

func NewJobRunInProgressEvent(jobRun *scheduler.JobRun) (*JobRunInProgress, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobRunInProgress{
		Event:  baseEvent,
		JobRun: jobRun,
	}, nil
}

func NewJobRunSuccessEvent(jobRun *scheduler.JobRun) (*JobRunSuccess, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobRunSuccess{
		Event:  baseEvent,
		JobRun: jobRun,
	}, nil
}

func NewJobRunFailedEvent(jobRun *scheduler.JobRun) (*JobRunFailed, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &JobRunFailed{
		Event:  baseEvent,
		JobRun: jobRun,
	}, nil
}

func toOptimusChangeEvent(j *scheduler.JobRun, e Event) *pbInt.OptimusChangeEvent {
	var eventType pbInt.OptimusChangeEvent_EventType
	switch j.State {
	case scheduler.StateWaitUpstream:
		eventType = pbInt.OptimusChangeEvent_EVENT_TYPE_JOB_WAIT_UPSTREAM
	case scheduler.StateInProgress:
		eventType = pbInt.OptimusChangeEvent_EVENT_TYPE_JOB_IN_PROGRESS
	case scheduler.StateSuccess:
		eventType = pbInt.OptimusChangeEvent_EVENT_TYPE_JOB_SUCCESS
	case scheduler.StateFailed:
		eventType = pbInt.OptimusChangeEvent_EVENT_TYPE_JOB_FAILURE
	}
	return &pbInt.OptimusChangeEvent{
		EventId:       e.ID.String(),
		OccurredAt:    timestamppb.New(e.OccurredAt),
		ProjectName:   j.Tenant.ProjectName().String(),
		NamespaceName: j.Tenant.NamespaceName().String(),
		EventType: eventType
		Payload: &pbInt.OptimusChangeEvent_JobRun{
			JobRun: &pbInt.JobRunPayload{
				JobName:     j.JobName.String(),
				ScheduledAt: timestamppb.New(j.ScheduledAt),
				JobRunId:    j.ID.String(),
			},
		},
	}
}
