package tenant

import (
	"fmt"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

const EntityTenant = "tenant"

type Tenant struct {
	projName ProjectName
	nsName   NamespaceName
}

func (t Tenant) ProjectName() ProjectName {
	return t.projName
}

func (t Tenant) String() string {
	return fmt.Sprintf("%s:%s", t.projName, t.nsName)
}

func (t Tenant) NamespaceName() NamespaceName {
	return t.nsName
}

func (t Tenant) IsInvalid() bool {
	return t.projName.String() == ""
}

func NewTenant(projectName, namespaceName string) (Tenant, error) {
	projName, err := ProjectNameFrom(projectName)
	if err != nil {
		return Tenant{}, err
	}

	nsName, err := NamespaceNameFrom(namespaceName)
	if err != nil {
		return Tenant{}, err
	}

	return Tenant{
		projName: projName,
		nsName:   nsName,
	}, nil
}

type WithDetails struct {
	project    Project
	namespace  Namespace
	secretsMap map[string]string
}

func NewTenantDetails(proj *Project, namespace *Namespace, secrets PlainTextSecrets) (*WithDetails, error) {
	if proj == nil {
		return nil, errors.InvalidArgument(EntityTenant, "project is nil")
	}
	if namespace == nil {
		return nil, errors.InvalidArgument(EntityTenant, "namespace is nil")
	}

	return &WithDetails{
		project:    *proj,
		namespace:  *namespace,
		secretsMap: secrets.ToSecretMap().ToMap(),
	}, nil
}

func (w *WithDetails) ToTenant() Tenant {
	return Tenant{
		projName: w.project.Name(),
		nsName:   w.namespace.Name(),
	}
}

func (w *WithDetails) GetConfig(key string) (string, error) {
	config, err := w.namespace.GetConfig(key)
	if err == nil {
		return config, nil
	}

	// key not present in namespace, check project
	config, err = w.project.GetConfig(key)
	if err == nil {
		return config, nil
	}

	return "", errors.NotFound(EntityTenant, "config not present in tenant "+key)
}

func (w *WithDetails) GetVariable(key string) (string, error) {
	variable, err := w.namespace.GetVariable(key)
	if err == nil {
		return variable, nil
	}

	// key not present in namespace, check project
	variable, err = w.project.GetVariable(key)
	if err == nil {
		return variable, nil
	}

	return "", errors.NotFound(EntityTenant, fmt.Sprintf("variable not present in tenant: %s", key))
}

func (w *WithDetails) GetConfigs() map[string]string {
	m1 := w.namespace.GetConfigs()
	return utils.MergeMaps(w.project.GetConfigs(), m1)
}

// GetVariables for now will merge tenant variables & tenant configs.
// Since we are moving to use tenant "variables" for job config / job asset compilation & discourage using config for the purpose,
// merging both is a temporary solution to support older behavior.
// Once we have all tenants migrated to use variables, we can remove the tenant config.
func (w *WithDetails) GetVariables() map[string]string {
	return utils.MergeMaps(
		w.project.GetConfigs(),
		w.namespace.GetConfigs(),
		w.project.GetVariables(),
		w.namespace.GetVariables(),
	)
}

func (w *WithDetails) Project() *Project {
	return &w.project
}

func (w *WithDetails) Namespace() *Namespace {
	return &w.namespace
}

func (w *WithDetails) SecretsMap() map[string]string {
	return w.secretsMap
}
