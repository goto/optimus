package larkdrive

import (
	"encoding/json"

	"github.com/goto/optimus/internal/errors"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

const (
	ErrCodeInvalidParam = 1061002
	ErrCodeNotFound     = 1061003
	ErrCodeForbidden    = 1061004
)

func ErrCodeToErrorType(codeError larkcore.CodeError) errors.ErrorType {
	switch codeError.Code {
	case ErrCodeInvalidParam:
		return errors.ErrInvalidArgument
	case ErrCodeNotFound:
		return errors.ErrNotFound
	case ErrCodeForbidden:
		return errors.ErrForbidden
	default:
		return errors.ErrInternalError
	}
}

type Credential struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

func NewCredentialFromSecret(secret string) (*Credential, error) {
	cred := &Credential{}
	if err := json.Unmarshal([]byte(secret), cred); err != nil {
		return nil, errors.AddErrContextWithType(err, errors.ErrInvalidArgument, EntityLarkDrive, "invalid secret format, expected JSON")
	}
	return cred, nil
}
