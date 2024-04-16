package resource

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib"
	"github.com/goto/optimus/internal/lib/labels"
)

const (
	EntityResource       = "resource"
	nameSectionSeparator = "."
)

type Metadata struct {
	Version     int32
	Description string
	Labels      labels.Labels
}

func (m *Metadata) Validate() error {
	if m == nil {
		return errors.InvalidArgument(EntityResource, "metadata is nil")
	}

	if m.Labels != nil {
		if err := m.Labels.Validate(); err != nil {
			msg := fmt.Sprintf("labels is invalid: %v", err)
			return errors.InvalidArgument(EntityResource, msg)
		}
	}

	return nil
}

type Name string

func NameFrom(name string) (Name, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityResource, "resource name is empty")
	}

	return Name(name), nil
}

func (n Name) String() string {
	return string(n)
}

type Resource struct {
	name Name

	kind  string
	store Store
	urn   lib.URN

	tenant tenant.Tenant

	spec     map[string]any
	metadata *Metadata

	status Status
}

func NewResource(fullName, kind string, store Store, tnnt tenant.Tenant, meta *Metadata, spec map[string]any) (*Resource, error) {
	name, err := NameFrom(fullName)
	if err != nil {
		return nil, err
	}

	if len(spec) == 0 {
		return nil, errors.InvalidArgument(EntityResource, "empty resource spec for "+fullName)
	}

	if err := meta.Validate(); err != nil {
		msg := fmt.Sprintf("metadata for %s is invalid: %v", fullName, err)
		return nil, errors.InvalidArgument(EntityResource, msg)
	}

	return &Resource{
		name:     name,
		kind:     kind,
		store:    store,
		tenant:   tnnt,
		spec:     spec,
		metadata: meta,
		status:   StatusUnknown,
	}, nil
}

func (r *Resource) Name() Name {
	return r.name
}

func (r *Resource) FullName() string {
	return r.name.String()
}

func (r *Resource) URN() lib.URN {
	return r.urn
}

func (r *Resource) UpdateURN(urn lib.URN) error {
	if r.urn.IsZero() {
		r.urn = urn
		return nil
	}

	return errors.InvalidArgument(EntityResource, "urn already present for "+r.FullName())
}

func (r *Resource) UpdateTenant(tnnt tenant.Tenant) {
	r.tenant = tnnt
}

func (r *Resource) Metadata() *Metadata {
	return r.metadata
}

func (r *Resource) NameSections() []string {
	return strings.Split(r.name.String(), nameSectionSeparator)
}

func (r *Resource) Kind() string {
	return r.kind
}

func (r *Resource) Tenant() tenant.Tenant {
	return r.tenant
}

func (r *Resource) Store() Store {
	return r.store
}

func (r *Resource) Status() Status {
	return r.status
}

func (r *Resource) Spec() map[string]any {
	return r.spec
}

func (r *Resource) Equal(incoming *Resource) bool {
	if r == nil || incoming == nil {
		return r == nil && incoming == nil
	}
	if r.name != incoming.name {
		return false
	}
	if r.kind != incoming.kind {
		return false
	}
	if r.store != incoming.store {
		return false
	}
	if !reflect.DeepEqual(r.tenant, incoming.tenant) {
		return false
	}
	if !reflect.DeepEqual(r.spec, incoming.spec) {
		return false
	}
	return reflect.DeepEqual(r.metadata, incoming.metadata)
}

type FromExistingOpt func(r *Resource)

func ReplaceStatus(status Status) FromExistingOpt {
	return func(r *Resource) {
		r.status = status
	}
}

func FromExisting(existing *Resource, opts ...FromExistingOpt) *Resource {
	output := &Resource{
		name:     existing.name,
		kind:     existing.kind,
		store:    existing.store,
		tenant:   existing.tenant,
		spec:     existing.spec,
		metadata: existing.metadata,
		urn:      existing.urn,
		status:   existing.status,
	}
	for _, opt := range opts {
		opt(output)
	}
	return output
}
