package plan

import (
	"fmt"
	"strings"
)

type ResourcePlan struct {
	Name         string  `json:"name"`
	Datastore    string  `json:"datastore"`
	OldNamespace *string `json:"old_namespace"` // OldNamespace will be used on migrate operation
}

func (p ResourcePlan) GetName() string { return ConstructResourceName(p.Datastore, p.Name) }

func (p *ResourcePlan) SetName(name string) { p.Name = name }

func (p *ResourcePlan) SetOldNamespace(oldNamespace string) { p.OldNamespace = &oldNamespace }

func (ResourcePlan) FileSuffix() string { return "/resource.yaml" }

func (p ResourcePlan) ValidDirectory(directory string) bool {
	return strings.HasSuffix(directory, p.FileSuffix())
}

func (p ResourcePlan) ParseDirectory(directory string) string {
	return strings.TrimSuffix(directory, p.FileSuffix())
}

func ConstructResourceName(datastore, name string) string {
	return fmt.Sprintf("%s://%s", datastore, name)
}
