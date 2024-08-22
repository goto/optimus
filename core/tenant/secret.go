package tenant

import (
	"strings"

	"github.com/goto/optimus/internal/errors"
)

const (
	EntitySecret = "secret"

	SecretStorageKey            = "STORAGE"
	SecretSchedulerAuth         = "SCHEDULER_AUTH"
	SecretNotifySlack           = "NOTIFY_SLACK"
	SecretLarkAppID             = "NOTIFY_LARK_APP_ID"
	SecretLarkAppSecret         = "NOTIFY_LARK_APP_SECRET"
	SecretLarkVerificationToken = "NOTIFY_LARK_VERIFICATION_TOKEN"
)

type SecretName string

func SecretNameFrom(name string) (SecretName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntitySecret, "secret name is empty")
	}
	return SecretName(strings.ToUpper(name)), nil
}

func (sn SecretName) String() string {
	return string(sn)
}

type PlainTextSecret struct {
	name  SecretName
	value string
}

func NewPlainTextSecret(name, value string) (*PlainTextSecret, error) {
	secretName, err := SecretNameFrom(name)
	if err != nil {
		return nil, err
	}

	if value == "" {
		return nil, errors.InvalidArgument(EntitySecret, "empty secret value")
	}

	return &PlainTextSecret{
		name:  secretName,
		value: value,
	}, nil
}

func (p *PlainTextSecret) Value() string {
	return p.value
}

func (p *PlainTextSecret) Name() SecretName {
	return p.name
}

type PlainTextSecrets []*PlainTextSecret

type SecretMap map[string]string

func (p PlainTextSecrets) ToSecretMap() SecretMap {
	secretMap := map[string]string{}
	for _, item := range p {
		secretMap[item.Name().String()] = item.Value()
	}
	return secretMap
}

func (s SecretMap) Get(secretName string) (string, error) {
	if secretName == "" {
		return "", errors.InvalidArgument(EntitySecret, "empty secret name")
	}

	if secret, ok := s[strings.ToUpper(secretName)]; ok {
		return secret, nil
	}
	return "", errors.NotFound(EntitySecret, "value not found for: "+secretName)
}

func (s SecretMap) ToMap() map[string]string {
	return s
}

type Secret struct {
	name         SecretName
	encodedValue string

	projName      ProjectName
	namespaceName string
}

func (s *Secret) Name() SecretName {
	return s.name
}

func (s *Secret) EncodedValue() string {
	return s.encodedValue
}

func (s *Secret) ProjectName() ProjectName {
	return s.projName
}

func (s *Secret) NamespaceName() string {
	return s.namespaceName
}

func NewSecret(name, encodedValue string, projName ProjectName, nsName string) (*Secret, error) {
	secretName, err := SecretNameFrom(name)
	if err != nil {
		return nil, err
	}

	if encodedValue == "" {
		return nil, errors.InvalidArgument(EntitySecret, "empty encoded secret")
	}

	if projName == "" {
		return nil, errors.InvalidArgument(EntitySecret, "invalid tenant details")
	}

	return &Secret{
		name:          secretName,
		encodedValue:  encodedValue,
		projName:      projName,
		namespaceName: nsName,
	}, nil
}
