package survey

import (
	"fmt"

	"github.com/AlecAivazis/survey/v2"
)

// JobDeleteSurvey defines survey for job deletion operation
type JobDeleteSurvey struct{}

func NewJobDeleteSurvey() *JobDeleteSurvey {
	return &JobDeleteSurvey{}
}

// AskToConfirmDeletion asks questions to confirm whether to delete the specified job or not
func (*JobDeleteSurvey) AskToConfirmDeletion(jobName string) (bool, error) {
	var result bool
	if err := survey.AskOne(&survey.Confirm{
		Message: fmt.Sprintf("Are you sure you want to delete [%s]?", jobName),
		Default: false,
	}, &result); err != nil {
		return false, err
	}

	return result, nil
}

// AskToConfirmForceDeletion asks questions to confirm whether a force delete to the specified job is to be done or not
func (*JobDeleteSurvey) AskToConfirmForceDeletion(jobName string) (bool, error) {
	var result bool
	if err := survey.AskOne(&survey.Confirm{
		Message: fmt.Sprintf("Are you sure you want to FORCE delete [%s]?", jobName),
		Help: `Force delete will remove the specified job regardless of its downstream.
  It means, the downstreams which depend on this job will fail.
  * If the reference is through inference, then refreshing those downstream should fix it.
  * If the reference is through static, then deletion to the static reference and refreshing it should fix.`,
		Default: false,
	}, &result); err != nil {
		return false, err
	}

	return result, nil
}
