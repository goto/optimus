package maxcompute

import (
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

const (
	EntityExternalTable = "resource_external_table"
)

type ExternalTable struct {
	Name resource.Name

	Description string          `mapstructure:"description,omitempty"`
	Schema      Schema          `mapstructure:"schema,omitempty"`
	Source      *ExternalSource `mapstructure:"source,omitempty"`

	Hints map[string]string `mapstructure:"hints,omitempty"`
}

func (e *ExternalTable) FullName() string {
	return e.Name.String()
}

func (e *ExternalTable) Validate() error {
	if len(e.Schema) > 0 {
		err := e.Schema.Validate()
		if err != nil {
			return errors.AddErrContext(err, EntityExternalTable, "error in schema for "+e.FullName())
		}
	}

	if e.Source == nil {
		return errors.InvalidArgument(EntityExternalTable, "empty external table source for "+e.FullName())
	}
	if err := e.Source.Validate(); err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "error in source for "+e.FullName())
	}
	return nil
}

type ExternalSource struct {
	SourceType string   `mapstructure:"type,omitempty"`
	SourceURIs []string `mapstructure:"uris,omitempty"`

	// Additional configs for CSV, GoogleSheets, LarkSheets formats.
	SerdeProperties map[string]string `mapstructure:"serde_properties"`
	TableProperties map[string]string `mapstructure:"table_properties"`

	SyncInterval     int64    `mapstructure:"sync_interval_in_hrs,omitempty"`
	GetFormattedDate bool     `mapstructure:"fetch_formatted_datetime,omitempty"`
	Jars             []string `mapstructure:"jars,omitempty"`
	Location         string   `mapstructure:"location,omitempty"`
	Range            string   `mapstructure:"range,omitempty"`
}

func (e ExternalSource) Validate() error {
	if e.SourceType == "" {
		return errors.InvalidArgument(EntityExternalTable, "source type is empty")
	}
	// TODO: Enable sourceURI validation with sheets
	// if  len(e.SourceURIs) == 0 {
	//	return errors.InvalidArgument(EntityExternalTable, "source uri list is empty")
	//}
	// for _, uri := range e.SourceURIs {
	//	if uri == "" {
	//		return errors.InvalidArgument(EntityExternalTable, "uri is empty")
	//	}
	//}

	return nil
}
