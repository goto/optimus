package plan

import "strings"

type JobPlan struct {
	Name         string  `json:"name"`
	OldNamespace *string `json:"old_namespace"`
}

func (p JobPlan) GetName() string { return p.Name }

func (p *JobPlan) SetName(name string) { p.Name = name }

func (p *JobPlan) SetOldNamespace(oldNamespace string) { p.OldNamespace = &oldNamespace }

func (JobPlan) FileSuffix() string { return "/job.yaml" }

func (p JobPlan) ValidDirectory(directory string) bool {
	index := strings.Index(directory, "/assets")
	return strings.HasSuffix(directory, p.FileSuffix()) && index < 1
}

func (p JobPlan) ParseDirectory(directory string) string {
	index := strings.Index(directory, "/assets")
	directory = strings.TrimSuffix(directory, p.FileSuffix())
	if index > 0 {
		directory = directory[:index]
	}
	return directory
}
