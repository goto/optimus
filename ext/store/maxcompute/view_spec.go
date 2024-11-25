package maxcompute

import (
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const (
	EntityView = "resource_view"
)

type View struct {
	Name resource.Name

	Description string   `mapstructure:"description,omitempty"`
	Columns     []string `mapstructure:"columns,omitempty"`
	ViewQuery   string   `mapstructure:"view_query,omitempty"`
	Lifecycle   int      `mapstructure:"lifecycle,omitempty"`
}

func (v *View) Validate() error {
	if v.ViewQuery == "" {
		return errors.InvalidArgument(EntityView, "view query is empty for "+v.Name.String())
	}

	return nil
}
