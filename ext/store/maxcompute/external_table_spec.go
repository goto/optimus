package maxcompute

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/goto/optimus/internal/errors"
)

const (
	EntityExternalTable = "resource_external_table"
)

type ExternalTable struct {
	Name     string `mapstructure:"name,omitempty"`
	Project  string `mapstructure:"project,omitempty"`
	Database string `mapstructure:"database,omitempty"`

	Description string          `mapstructure:"description,omitempty"`
	Schema      Schema          `mapstructure:"schema,omitempty"`
	Source      *ExternalSource `mapstructure:"source,omitempty"`

	Hints map[string]string `mapstructure:"hints,omitempty"`
}

func (e *ExternalTable) FullName() string {
	return fmt.Sprintf("%s.%s.%s", e.Project, e.Database, e.Name)
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
	SourceType  string   `mapstructure:"type,omitempty"`
	ContentType string   `mapstructure:"content_type,omitempty"`
	SourceURIs  []string `mapstructure:"uris,omitempty"`

	// Additional configs for CSV, GoogleSheets, LarkSheets formats.
	SerdeProperties map[string]string `mapstructure:"serde_properties"`
	TableProperties map[string]string `mapstructure:"table_properties"`

	SyncInterval     int64    `mapstructure:"sync_interval_in_hrs,omitempty"`
	GetFormattedDate bool     `mapstructure:"fetch_formatted_datetime,omitempty"`
	GetFormattedData bool     `mapstructure:"fetch_formatted_data,omitempty"`
	CleanGDriveCSV   bool     `mapstructure:"clean_gdrive_csv,omitempty"`
	Jars             []string `mapstructure:"jars,omitempty"`
	Location         string   `mapstructure:"location,omitempty"`
	Range            string   `mapstructure:"range,omitempty"`
}

func (e ExternalSource) GetHeaderCount() (int, error) {
	headers := 0
	if val, ok := e.SerdeProperties[headersCountSerde]; ok && val != "" {
		num, err := strconv.Atoi(val)
		if err != nil {
			return 0, errors.InvalidArgument(EntityExternalTable, "unable to parse "+headersCountSerde)
		}
		headers = num
	}
	return headers, nil
}

func (e ExternalSource) Validate() error {
	switch strings.ToUpper(e.SourceType) {
	case GoogleSheet, GoogleDrive:
		if len(e.SourceURIs) == 0 {
			return errors.InvalidArgument(EntityExternalTable, "source uri list is empty")
		}
		for _, uri := range e.SourceURIs {
			if uri == "" {
				return errors.InvalidArgument(EntityExternalTable, "uri is empty")
			}
		}
		return nil
	case "":
		return errors.InvalidArgument(EntityExternalTable, "source type is empty")
	default:
		return errors.InvalidArgument(EntityExternalTable, fmt.Sprintf("got: [%s]", e.SourceType))
	}
}
