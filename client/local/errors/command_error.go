package errors

import "fmt"

const (
	ExitCodeWarn            = 10
	ExitCodeValidationError = 30
)

// CmdError is a custom error type for command errors, it will contain the error and the exit code
type CmdError struct {
	Cause error
	Code  int
}

func (e *CmdError) Error() string { return e.Cause.Error() }

func NewCmdError(cause error, code int) *CmdError {
	return &CmdError{
		Cause: cause,
		Code:  code,
	}
}

func NewWarnErrorf(format string, args ...any) *CmdError {
	return NewCmdError(fmt.Errorf(format, args...), ExitCodeWarn)
}

func NewValidationErrorf(format string, args ...any) *CmdError {
	return NewCmdError(fmt.Errorf(format, args...), ExitCodeValidationError)
}
