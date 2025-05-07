package resource

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type Deprecated struct {
	Reason           string
	Date             time.Time
	ReplacementTable string
}

type Resource struct {
	ID uuid.UUID

	FullName string
	Kind     string
	Store    string

	ProjectName   string
	NamespaceName string

	Metadata json.RawMessage
	Spec     map[string]any

	Deprecated json.RawMessage

	URN string

	Status string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func FromResourceToModel(r *resource.Resource) *Resource {
	metadata, _ := json.Marshal(r.Metadata())
	deprecation, _ := json.Marshal(r.GetDeprecationInfo())

	return &Resource{
		FullName:      r.FullName(),
		Kind:          r.Kind(),
		Store:         r.Store().String(),
		ProjectName:   r.Tenant().ProjectName().String(),
		NamespaceName: r.Tenant().NamespaceName().String(),
		Metadata:      metadata,
		Spec:          r.Spec(),
		URN:           r.URN().String(),
		Status:        r.Status().String(),
		Deprecated:    deprecation,
	}
}

func FromModelToResource(r *Resource) (*resource.Resource, error) {
	store, err := resource.FromStringToStore(r.Store)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error constructing kind", err)
	}
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, errors.Wrap(tenant.EntityTenant, "error constructing new tenant", err)
	}
	var metadata *resource.Metadata
	if err := json.Unmarshal(r.Metadata, &metadata); err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error unmarshalling metadata", err)
	}

	var deprecated *resource.Deprecated
	if err := json.Unmarshal(r.Deprecated, &deprecated); err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error unmarshalling deprecation info", err)
	}

	output, err := resource.NewResource(r.FullName, r.Kind, store, tnnt, metadata, r.Spec, deprecated)
	if err == nil {
		output = resource.FromExisting(output, resource.ReplaceStatus(resource.FromStringToStatus(r.Status)))

		var urn resource.URN
		if r.URN != "" {
			tempURN, err := resource.ParseURN(r.URN)
			if err != nil {
				return nil, err
			}

			urn = tempURN
		}

		output.UpdateURN(urn)
	}
	return output, err
}
