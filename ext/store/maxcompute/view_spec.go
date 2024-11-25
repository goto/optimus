package maxcompute

import (
	"fmt"

	"github.com/goto/optimus/internal/errors"
)

const (
	EntityView = "resource_view"
)

type View struct {
	Name     string `mapstructure:"name,omitempty"`
	Project  string `mapstructure:"project,omitempty"`
	Database string `mapstructure:"database,omitempty"`

	Description string   `mapstructure:"description,omitempty"`
	Columns     []string `mapstructure:"columns,omitempty"`
	ViewQuery   string   `mapstructure:"view_query,omitempty"`
	Lifecycle   int      `mapstructure:"lifecycle,omitempty"`
}

func (v *View) FullName() string {
	return fmt.Sprintf("%s.%s.%s", v.Project, v.Database, v.Name)
}

func (v *View) Validate() error {
	if v.ViewQuery == "" {
		return errors.InvalidArgument(EntityView, "view query is empty for "+v.FullName())
	}

	return nil
}
