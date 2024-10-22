package maxcompute

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

var mcResourceURNRegex = regexp.MustCompile(`maxcompute://([^:]+).([^.]+)\.(.+)`)

type ResourceURN struct {
	Project string `mapstructure:"project"`
	Schema  string `mapstructure:"database"`
	Name    string `mapstructure:"name"`
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
		return spec, err
	}

	return spec, nil
}

func getComponentName(res *resource.Resource) (resource.Name, error) {
	component, err := getURNComponent(res)
	if err != nil {
		return "", err
	}
	return resource.Name(component.Name), nil
}

func resourceNameFor(name resource.Name) (string, error) {
	parts := strings.Split(name.String(), ".")
	if len(parts) < 3 {
		return "", errors.InvalidArgument(resource.EntityResource, "invalid resource name: "+name.String())
	}

	return parts[2], nil
}
