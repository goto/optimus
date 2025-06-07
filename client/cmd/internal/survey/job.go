package survey

import (
	"github.com/AlecAivazis/survey/v2"

	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
)

// JobSurvey defines survey for job specification in general
type JobSurvey struct{}

// NewJobSurvey initializes job survey
func NewJobSurvey() *JobSurvey {
	return &JobSurvey{}
}

// AskToSelectJobName asks to select job name
func (*JobSurvey) AskToSelectJobName(jobSpecReader local.SpecReader[*model.JobSpec], jobDirPath string) (string, error) {
	jobs, err := jobSpecReader.ReadAll(jobDirPath)
	if err != nil {
		return "", err
	}
	var allJobNames []string
	for _, job := range jobs {
		allJobNames = append(allJobNames, job.Name)
	}
	var selectedJobName string
	if err := survey.AskOne(&survey.Select{
		Message: "Select a Job",
		Options: allJobNames,
	}, &selectedJobName); err != nil {
		return "", err
	}
	return selectedJobName, nil
}
