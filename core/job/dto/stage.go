package dto

const (
	StageCyclicValidation            ValidateStage = "cyclic validation"
	StageDeletionValidation          ValidateStage = "validation for deletion"
	StageDestinationValidation       ValidateStage = "destination validation"
	StagePreparation                 ValidateStage = "validation preparation"
	StageRunCompileValidation        ValidateStage = "compile validation for run"
	StageSourceValidation            ValidateStage = "source validation"
	StageTenantValidation            ValidateStage = "tenant validation"
	StageUpstreamValidation          ValidateStage = "upstream validation"
	StageWindowValidation            ValidateStage = "window validation"
	StageSourceDeprecationValidation ValidateStage = "source validation for deprecation"
)

type ValidateStage string

func (v ValidateStage) String() string {
	return string(v)
}

func (v ValidateStage) Equal(compare string) bool { return string(v) == compare }
