package maxcompute

import (
	"fmt"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const (
	EntityProject  = "project"
	EntitySchema   = "schema"
	EntityFunction = "function"

	ProjectSchemaSections = 2
	TableNameSections     = 3
)

type ResourceURN struct {
	Project string `mapstructure:"project"`
	Schema  string `mapstructure:"database"`
	Name    string `mapstructure:"name"`
}

func NewResourceURNFromResourceName(resourceName string) (ResourceURN, error) {
	parts := strings.Split(resourceName, ".")
	if len(parts) < TableNameSections {
		return ResourceURN{}, errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+resourceName)
	}

	return ResourceURN{
		Project: parts[0],
		Schema:  parts[1],
		Name:    parts[2],
	}, nil
}

func NewResourceURN(project, schema, name string) (ResourceURN, error) {
	me := errors.NewMultiError("resource urn constructor errors")
	if project == "" {
		me.Append(fmt.Errorf("project is empty"))
	}
	if schema == "" {
		me.Append(fmt.Errorf("schema is empty"))
	}
	if name == "" {
		me.Append(fmt.Errorf("name is empty"))
	}

	if len(me.Errors) > 0 {
		return ResourceURN{}, me.ToErr()
	}

	return ResourceURN{
		Project: project,
		Schema:  schema,
		Name:    name,
	}, nil
}

func (n ResourceURN) URN() string {
	return "maxcompute://" + fmt.Sprintf("%s.%s.%s", n.Project, n.Schema, n.Name)
}

type ResourceURNWithUpstreams struct {
	ResourceURN ResourceURN
	Upstreams   []*ResourceURNWithUpstreams
}

type ResourceURNWithUpstreamsList []*ResourceURNWithUpstreams

func (rs ResourceURNWithUpstreamsList) FlattenUnique() []*ResourceURNWithUpstreams {
	var output []*ResourceURNWithUpstreams
	for _, r := range rs {
		if r == nil {
			continue
		}
		newResource := *r
		newResource.Upstreams = nil
		nested := ResourceURNWithUpstreamsList(r.Upstreams).FlattenUnique()
		output = append(output, &newResource)
		output = append(output, nested...)
	}

	return ResourceURNWithUpstreamsList(output).unique()
}

func (rs ResourceURNWithUpstreamsList) unique() ResourceURNWithUpstreamsList {
	mapUnique := map[ResourceURN]*ResourceURNWithUpstreams{}
	for _, r := range rs {
		mapUnique[r.ResourceURN] = r
	}

	output := make([]*ResourceURNWithUpstreams, len(mapUnique))
	i := 0
	for _, u := range mapUnique {
		output[i] = u
		i++
	}
	return output
}

type ProjectSchema struct {
	Project string
	Schema  string
}

func ProjectSchemaFrom(project, schemaName string) (ProjectSchema, error) {
	if project == "" {
		return ProjectSchema{}, errors.InvalidArgument(EntityProject, "maxcompute project name is empty")
	}

	if schemaName == "" {
		return ProjectSchema{}, errors.InvalidArgument(EntitySchema, "maxcompute schema name is empty")
	}

	return ProjectSchema{
		Project: project,
		Schema:  schemaName,
	}, nil
}

func (ps ProjectSchema) FullName() string {
	return ps.Project + "." + ps.Schema
}

func ProjectSchemaFor(name resource.Name) (ProjectSchema, error) {
	parts := strings.Split(name.String(), ".")
	if len(parts) < ProjectSchemaSections {
		return ProjectSchema{}, errors.InvalidArgument(EntitySchema, "invalid schema name: "+name.String())
	}

	return ProjectSchemaFrom(parts[0], parts[1])
}

type ResourceURNs []ResourceURN

func (n ResourceURNs) GroupByProjectschema() map[ProjectSchema][]string {
	output := make(map[ProjectSchema][]string)

	for _, resourceURN := range n {
		pd := ProjectSchema{Project: resourceURN.Project, Schema: resourceURN.Schema}
		if _, ok := output[pd]; !ok {
			output[pd] = []string{}
		}
		output[pd] = append(output[pd], resourceURN.Name)
	}

	return output
}

func URNFor(res *resource.Resource) (resource.URN, error) {
	spec, err := getURNComponent(res)
	if err != nil {
		return resource.ZeroURN(), errors.InvalidArgument(resource.EntityResource, "not able to decode spec")
	}

	name := spec.Project + "." + spec.Schema + "." + spec.Name

	return resource.NewURN(resource.MaxCompute.String(), name)
}

func getURNComponent(res *resource.Resource) (ResourceURN, error) {
	var spec ResourceURN
	if err := mapstructure.Decode(res.Spec(), &spec); err != nil {
		msg := fmt.Sprintf("%s: not able to decode spec for %s", err, res.FullName())
		return spec, errors.InvalidArgument(resource.EntityResource, msg)
	}

	return spec, nil
}

func getCompleteComponentName(res *resource.Resource) (ProjectSchema, resource.Name, error) { //nolint: unparam
	if res.Version() == resource.ResourceSpecV2 {
		mcURN, err := getURNComponent(res)
		if err != nil {
			return ProjectSchema{}, "", err
		}

		projectSchema, err := ProjectSchemaFrom(mcURN.Project, mcURN.Schema)
		if err != nil {
			return ProjectSchema{}, "", err
		}

		return projectSchema, resource.Name(mcURN.Name), nil
	}

	projectSchema, err := ProjectSchemaFor(res.Name())
	if err != nil {
		return ProjectSchema{}, "", err
	}

	resourceName, err := resourceNameFor(res.Name(), res.Kind())
	if err != nil {
		return ProjectSchema{}, "", err
	}

	return projectSchema, resource.Name(resourceName), nil
}

func resourceNameFor(name resource.Name, kind string) (string, error) {
	parts := strings.Split(name.String(), ".")

	if kind == KindSchema {
		if len(parts) < ProjectSchemaSections {
			return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
		}

		if parts[1] == "" {
			return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
		}
		return parts[1], nil
	}

	if len(parts) < TableNameSections {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
	}

	if parts[2] == "" {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
	}

	return parts[2], nil
}
