package tenant

import (
	"github.com/goto/optimus/internal/errors"
)

const EntityNamespace = "namespace"

type NamespaceName string

func NamespaceNameFrom(name string) (NamespaceName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityNamespace, "namespace name is empty")
	}
	// TODO: add condition, that namespace name should not have "." as this will break the standard URN design
	return NamespaceName(name), nil
}

func (n NamespaceName) String() string {
	return string(n)
}

type Namespace struct {
	name NamespaceName

	projectName ProjectName
	config      map[string]string
	variables   map[string]string
}

func (n *Namespace) Name() NamespaceName {
	return n.name
}

func (n *Namespace) ProjectName() ProjectName {
	return n.projectName
}

func (n *Namespace) GetConfig(key string) (string, error) {
	for k, v := range n.config {
		if key == k {
			return v, nil
		}
	}
	return "", errors.NotFound(EntityNamespace, "namespace config not found "+key)
}

// GetConfigs returns a clone on project configurations
func (n *Namespace) GetConfigs() map[string]string {
	confs := make(map[string]string, len(n.config))
	for k, v := range n.config {
		confs[k] = v
	}
	return confs
}

// GetVariables returns a clone of namespace variables
func (n *Namespace) GetVariables() map[string]string {
	vars := make(map[string]string, len(n.variables))
	for k, v := range n.variables {
		vars[k] = v
	}
	return vars
}

func NewNamespace(name string, projName ProjectName, config map[string]string) (*Namespace, error) {
	nsName, err := NamespaceNameFrom(name)
	if err != nil {
		return nil, err
	}

	if projName == "" {
		return nil, errors.InvalidArgument(EntityNamespace, "project name is empty")
	}

	return &Namespace{
		name:        nsName,
		config:      config,
		projectName: projName,
	}, nil
}
