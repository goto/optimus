package event

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/tenant"
	pbInt "github.com/goto/optimus/protos/gotocompany/optimus/integration/v1beta1"
)

const (
	Announcement   = "announcement"
	UpstreamChange = "upstream_change"
)

type CreateSubscription struct {
	Event

	Owner      string
	EventTypes []string
	tenant     tenant.Tenant
	URN        string
}

func NewCreateSubscriptionEvent(tnnt tenant.Tenant, urn string, eventType []string) (*CreateSubscription, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &CreateSubscription{
		Event:      baseEvent,
		Owner:      "data-batching@gojek.com", // need to decide does this have to be job owner or data-batching@gojek.com
		EventTypes: eventType,
		tenant:     tnnt,
		URN:        urn,
	}, nil
}

func (c *CreateSubscription) Bytes() ([]byte, error) {
	optEvent := &pbInt.OptimusChangeEvent{
		EventId:       c.Event.ID.String(),
		OccurredAt:    timestamppb.New(c.Event.OccurredAt),
		ProjectName:   c.tenant.ProjectName().String(),
		NamespaceName: c.tenant.NamespaceName().String(),
		EventType:     pbInt.OptimusChangeEvent_EVENT_TYPE_CREATE_SUBSCRIPTION,
		Payload: &pbInt.OptimusChangeEvent_CreateSubscription{
			CreateSubscription: &pbInt.CreateSubscription{
				Owner:        c.Owner,
				EventTypes:   c.EventTypes,
				GroupSlug:    c.tenant.NamespaceName().String(),
				ProjectSlug:  c.tenant.ProjectName().String(),
				ResourceId:   c.URN,
				ResourceType: "optimus",
			},
		},
	}
	return proto.Marshal(optEvent)
}
