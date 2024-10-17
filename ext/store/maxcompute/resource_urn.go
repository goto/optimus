package maxcompute

import (
	"fmt"
	"github.com/goto/optimus/core/resource"
	"regexp"

	"github.com/goto/optimus/internal/errors"
)

const (
	EntityProject = "project"
	EntitySchema  = "schema"
)

var mcResourceURNRegex = regexp.MustCompile(`maxcompute://([^:]+):([^\.]+)\.(.+)`)

type ResourceURN struct {
	project string
	schema  string
	name    string
}

func NewResourceURNFromString(urn string) (ResourceURN, error) {
	const lengthMatchedString = 3
	matchedString := mcResourceURNRegex.FindStringSubmatch(urn)
	if len(matchedString) != lengthMatchedString+1 {
		return ResourceURN{}, fmt.Errorf("urn %s can't be parsed to maxcompute urn format", urn)
	}
	project, schema, table := matchedString[1], matchedString[2], matchedString[3]
	return ResourceURN{project: project, schema: schema, name: table}, nil
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
		project: project,
		schema:  schema,
		name:    name,
	}, nil
}

func (n ResourceURN) URN() string {
	return "maxcompute://" + fmt.Sprintf("%s:%s.%s", n.project, n.schema, n.name)
}

func (n ResourceURN) Project() string {
	return n.project
}

func (n ResourceURN) Schema() string {
	return n.schema
}

func (n ResourceURN) Name() string {
	return n.name
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

type ResourceURNs []ResourceURN

func (n ResourceURNs) GroupByProjectschema() map[ProjectSchema][]string {
	output := make(map[ProjectSchema][]string)

	for _, resourceURN := range n {
		pd := ProjectSchema{Project: resourceURN.project, Schema: resourceURN.schema}
		if _, ok := output[pd]; !ok {
			output[pd] = []string{}
		}
		output[pd] = append(output[pd], resourceURN.name)
	}

	return output
}

func ProjectSchemaFrom(project, schema string) (ProjectSchema, error) {
	if project == "" {
		return ProjectSchema{}, errors.InvalidArgument(EntityProject, "maxcompute project name is empty")
	}

	if schema == "" {
		return ProjectSchema{}, errors.InvalidArgument(EntitySchema, "maxcompute schema name is empty")
	}

	return ProjectSchema{
		Project: project,
		Schema:  schema,
	}, nil
}

func SchemaFor(name resource.Name) (ProjectSchema, error) {
	sections := name.Sections()
	if len(sections) < SchemaNameSections {
		return ProjectSchema{}, errors.InvalidArgument(EntitySchema, "invalid schema name: "+name.String())
	}

	return ProjectSchemaFrom(sections[0], sections[1])
}

func generateMaxComputeURN(res *resource.Resource) (resource.URN, error) {
	schema, err := SchemaFor(res.Name())
	if err != nil {
		return resource.ZeroURN(), err
	}

	if res.Kind() == KindSchema {
		name := schema.Project + ":" + schema.Schema
		return resource.NewURN(resource.MaxCompute.String(), name)
	}

	resourceName, err := resourceNameFor(res.Name(), res.Kind())
	if err != nil {
		return resource.ZeroURN(), err
	}

	name := schema.Project + ":" + schema.Schema + "." + resourceName
	return resource.NewURN(resource.MaxCompute.String(), name)
}

func resourceNameFor(name resource.Name, kind string) (string, error) {
	sections := name.Sections()
	var strName string
	if kind == KindSchema {
		if len(sections) < SchemaNameSections {
			return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
		}
		strName = sections[1]
	} else {
		if len(sections) < TableNameSections {
			return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
		}
		strName = sections[2]
	}

	if strName == "" {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
	}
	return strName, nil
}
