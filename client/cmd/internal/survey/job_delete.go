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

// AskToConfirm asks question to confirm whether to delete the specified job or not
func (*JobDeleteSurvey) AskToConfirm(jobName string) (bool, error) {
	var result bool
	if err := survey.AskOne(&survey.Confirm{
		Message: fmt.Sprintf("Are you sure you want to delete [%s]?", jobName),
		Default: false,
	}, &result); err != nil {
		return false, err
	}

	return result, nil
}

// AskToConfirmForce asks question to confirm whether a force delete to the specified job is to be done or not
func (*JobDeleteSurvey) AskToConfirmForce(jobName string) (bool, error) {
	var result bool
	if err := survey.AskOne(&survey.Confirm{
		Message: fmt.Sprintf("Are you really sure you want to FORCE delete [%s]?", jobName),
		Help: `FORCE delete will remove the specified job regardless of its downstream.
  It means, the downstreams which depend on this job will fail.
  * If the reference is through inference, then refreshing those downstream should fix it.
  * If the reference is through static, then deletion to the static reference and refreshing it should fix.`,
		Default: false,
	}, &result); err != nil {
		return false, err
	}

	return result, nil
}

// AskToConfirmCleanHistory asks question to confirm whether a clean history delete to the specified job is to be done or not
func (*JobDeleteSurvey) AskToConfirmCleanHistory(jobName string) (bool, error) {
	var result bool
	if err := survey.AskOne(&survey.Confirm{
		Message: fmt.Sprintf("Are you really sure you want to delete with CLEAN HISTORY for job [%s]?", jobName),
		Help: `CLEAN HISTORY will hard delete the specified job from the database server.
  This means that it will be impossible to use this job once it's deleted from the database.`,
		Default: false,
	}, &result); err != nil {
		return false, err
	}

	return result, nil
}
