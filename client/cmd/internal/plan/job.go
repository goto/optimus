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
	return strings.HasSuffix(directory, p.FileSuffix()) || index > 0
}

func (p JobPlan) ParseDirectory(directory string) string {
	if strings.HasSuffix(directory, p.FileSuffix()) {
		return strings.TrimSuffix(directory, p.FileSuffix())
	}

	index := strings.Index(directory, "/assets")
	if index > 0 {
		return directory[:index]
	}

	return ""
}
