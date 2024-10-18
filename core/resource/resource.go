package resource

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/labels"
)

const (
	EntityResource       = "resource"
	nameSectionSeparator = "."

	UnspecifiedImpactChange    UpdateImpact = "unspecified_impact"
	ResourceDataPipeLineImpact UpdateImpact = "data_impact"

	ResourceSpecV1 = 1
	ResourceSpecV2 = 2

	DefaultResourceSpecVersion = ResourceSpecV1
)

type UpdateImpact string

type Metadata struct {
	Version     int32
	Description string
	Labels      labels.Labels
}

type ChangeType string

const (
	ChangeTypeUpdate ChangeType = "Modified"
	ChangeTypeDelete ChangeType = "Deleted"
)

func (j ChangeType) String() string {
	return string(j)
}

type AlertAttrs struct {
	Name      Name
	URN       string
	Tenant    tenant.Tenant
	EventTime time.Time
	EventType ChangeType
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

	cleaned := strings.ReplaceAll(name, ":", ".") // TODO: design flaw, needs to be refactored

	return Name(cleaned), nil
}

func (n Name) Sections() []string {
	return strings.Split(n.String(), nameSectionSeparator)
}

func (n Name) String() string {
	return string(n)
}

type Resource struct {
	name Name

	kind  string
	store Store
	urn   URN

	tenant tenant.Tenant

	spec     map[string]any
	metadata *Metadata

	status Status
}

func (r *Resource) GetUpdateImpact(incoming *Resource) UpdateImpact {
	if !reflect.DeepEqual(r.spec, incoming.spec) {
		return ResourceDataPipeLineImpact
	}
	return UnspecifiedImpactChange
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

func (r *Resource) ConsoleURN() string {
	resourceProject := strings.Split(r.urn.name, ":")[0]
	return fmt.Sprintf("urn:%s:%s:%s:%s", r.store.String(), resourceProject, r.kind, r.urn.name)
}

func (r *Resource) URN() URN {
	return r.urn
}

func (r *Resource) UpdateURN(urn URN) error {
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

func (r *Resource) Version() int32 {
	if r.metadata == nil || r.metadata.Version == 0 {
		return DefaultResourceSpecVersion
	}

	return r.metadata.Version
}

func (r *Resource) UpdateSpec(spec map[string]any) {
	r.spec = spec
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

func (r *Resource) IsDeleted() bool { return r.status == StatusDeleted }

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
