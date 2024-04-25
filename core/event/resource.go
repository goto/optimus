package event

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	pbCore "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	pbInt "github.com/goto/optimus/protos/gotocompany/optimus/integration/v1beta1"
)

type ResourceCreated struct {
	Event

	Resource *resource.Resource
}

func NewResourceCreatedEvent(rsc *resource.Resource) (*ResourceCreated, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &ResourceCreated{
		Event:    baseEvent,
		Resource: rsc,
	}, nil
}

func (r ResourceCreated) Bytes() ([]byte, error) {
	return resourceEventToBytes(r.Event, r.Resource, pbInt.OptimusChangeEvent_EVENT_TYPE_RESOURCE_CREATE, pbInt.ChangeImpact_CHANGE_IMPACT_TYPE_UNSPECIFIED)
}

type ResourceUpdated struct {
	Event

	Resource     *resource.Resource
	UpdateImpact pbInt.ChangeImpact
}

func NewResourceUpdatedEvent(rsc *resource.Resource, impact resource.UpdateImpact) (*ResourceUpdated, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	var impactProto pbInt.ChangeImpact
	switch impact {
	case resource.ResourceDataPipeLineImpact:
		impactProto = pbInt.ChangeImpact_CHANGE_IMPACT_TYPE_BEHAVIOUR
	default:
		impactProto = pbInt.ChangeImpact_CHANGE_IMPACT_TYPE_UNSPECIFIED
	}

	return &ResourceUpdated{
		Event:        baseEvent,
		Resource:     rsc,
		UpdateImpact: impactProto,
	}, nil
}

func (r ResourceUpdated) Bytes() ([]byte, error) {
	return resourceEventToBytes(r.Event, r.Resource, pbInt.OptimusChangeEvent_EVENT_TYPE_RESOURCE_UPDATE, r.UpdateImpact)
}

type ResourceDeleted struct {
	Event

	Resource *resource.Resource
}

func NewResourceDeleteEvent(rsc *resource.Resource) (*ResourceDeleted, error) {
	baseEvent, err := NewBaseEvent()
	if err != nil {
		return nil, err
	}
	return &ResourceDeleted{
		Event:    baseEvent,
		Resource: rsc,
	}, nil
}

func (r ResourceDeleted) Bytes() ([]byte, error) {
	return resourceEventToBytes(r.Event, r.Resource, pbInt.OptimusChangeEvent_EVENT_TYPE_RESOURCE_DELETE, pbInt.ChangeImpact_CHANGE_IMPACT_TYPE_UNSPECIFIED)
}

func resourceEventToBytes(event Event, rsc *resource.Resource, eventType pbInt.OptimusChangeEvent_EventType, updateImpact pbInt.ChangeImpact) ([]byte, error) {
	meta := rsc.Metadata()
	if meta == nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "missing resource metadata")
	}

	pbStruct, err := structpb.NewStruct(rsc.Spec())
	if err != nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "unable to convert spec to proto struct")
	}

	resourcePb := &pbCore.ResourceSpecification{
		Version: meta.Version,
		Name:    rsc.FullName(),
		Type:    rsc.Kind(),
		Spec:    pbStruct,
		Assets:  nil,
		Labels:  meta.Labels,
	}
	occurredAt := timestamppb.New(event.OccurredAt)
	optEvent := &pbInt.OptimusChangeEvent{
		EventId:       event.ID.String(),
		OccurredAt:    occurredAt,
		ProjectName:   rsc.Tenant().ProjectName().String(),
		NamespaceName: rsc.Tenant().NamespaceName().String(),
		EventType:     eventType,
		Payload: &pbInt.OptimusChangeEvent_ResourceChange{
			ResourceChange: &pbInt.ResourceChangePayload{
				DatastoreName: rsc.Store().String(),
				Resource:      resourcePb,
				ChangeImpact:  updateImpact,
			},
		},
	}

	return proto.Marshal(optEvent)
}
