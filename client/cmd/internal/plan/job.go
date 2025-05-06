package plan

import "strings"

type JobPlan struct {
	Name         string  `json:"name"`
	OldNamespace *string `json:"old_namespace"`
	Path         string  `json:"path"`
}

func (p JobPlan) GetName() string { return p.Name }

func (p *JobPlan) SetName(name string) { p.Name = name }

func (p *JobPlan) SetOldNamespace(oldNamespace string) { p.OldNamespace = &oldNamespace }

func (p JobPlan) GetPath() string { return p.Path }

func (p *JobPlan) SetPath(path string) { p.Path = path }

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
