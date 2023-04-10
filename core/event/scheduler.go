package event

import (
	"github.com/gogo/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	pbInt "github.com/goto/optimus/protos/gotocompany/optimus/integration/v1beta1"
)

type JobRun struct {
	Event

	Run *scheduler.JobRun
}

func (j JobRun) Bytes() ([]byte, error) {
	occurredAt := timestamppb.New(j.Event.OccurredAt)
	optEvent := &pbInt.OptimusChangeEvent{
		EventId:    j.Event.ID.String(),
		OccurredAt: occurredAt,
		Payload: &pbInt.OptimusChangeEvent_JobRun{
			JobRun: &pbInt.JobRunPayload{
				JobRunId:    j.Run.ID.String(),
				JobName:     j.Run.JobName.String(),
				ScheduledAt: j.Run.ScheduledAt.String(),
			},
		},
	}
	return proto.Marshal(optEvent)
}
