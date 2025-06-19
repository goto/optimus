package maxcompute

import (
	"fmt"
	"strconv"
	"strings"
	"time"

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

func (e *ExternalTable) GetSyncDelayTolerance() time.Duration {
	if e.Source != nil {
		return time.Duration(e.Source.SyncInterval) * time.Hour
	}
	return 0
}

func (e *ExternalTable) GetSourceType() ExternalTableSourceType {
	return e.Source.SourceType
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

type ExternalTableSourceType string

func (e ExternalTableSourceType) String() string {
	return string(e)
}

func NewExternalTableSource(s string) (ExternalTableSourceType, error) {
	switch strings.ToUpper(s) {
	case GoogleSheet.String():
		return GoogleSheet, nil
	case LarkSheet.String():
		return LarkSheet, nil
	case GoogleDrive.String():
		return GoogleDrive, nil
	case OSS.String():
		return OSS, nil
	default:
		return "", errors.InvalidArgument(EntityExternalTable, "unsupported external table source :"+s)
	}
}

type ExternalTableSources []ExternalTableSourceType

func (es *ExternalTableSources) Has(sources ...ExternalTableSourceType) bool {
	for _, s := range *es {
		for _, source := range sources {
			if s == source {
				return true
			}
		}
	}
	return false
}

func (es *ExternalTableSources) Append(source ExternalTableSourceType) {
	if !es.Has(source) {
		*es = append(*es, source)
	}
}

type ExternalSource struct {
	SourceType  ExternalTableSourceType `mapstructure:"type,omitempty"`
	ContentType string                  `mapstructure:"content_type,omitempty"`
	SourceURIs  []string                `mapstructure:"uris,omitempty"`

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
	switch e.SourceType {
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
	case OSS:
		if len(e.SourceURIs) != 0 {
			return errors.InvalidArgument(EntityExternalTable, "source uri list is not empty, "+
				"Found `Source.Type` `OSS`. For `Source.Type` `OSS`, server only reads from `Source.Location` or default locations configured in Project Config, `ext_location`")
		}
		return nil
	case LarkSheet:
		if len(e.SourceURIs) == 0 {
			return errors.InvalidArgument(EntityExternalTable, "source uri list is empty")
		}
		// todo: check if more validations are possible
		return nil
	case "":
		return errors.InvalidArgument(EntityExternalTable, "source type is empty")
	default:
		return errors.InvalidArgument(EntityExternalTable, fmt.Sprintf("got: [%s]", e.SourceType))
	}
}
