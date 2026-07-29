package config

import "encoding/json"

type UpstreamResolverType string

const (
	DexUpstreamResolver UpstreamResolverType = "dex"
)

type ServerConfig struct {
	Version                   Version                   `mapstructure:"version"`
	Log                       LogConfig                 `mapstructure:"log"`
	Serve                     Serve                     `mapstructure:"serve"`
	Telemetry                 TelemetryConfig           `mapstructure:"telemetry"`
	Alerting                  AlertingConfig            `mapstructure:"alerting"`
	SLAConfig                 SLAConfig                 `mapstructure:"sla"`
	ResourceManagers          []ResourceManager         `mapstructure:"resource_managers"`
	UpstreamResolvers         []UpstreamResolver        `mapstructure:"upstream_resolvers"`
	Replay                    ReplayConfig              `mapstructure:"replay"`
	Backfill                  BackfillConfig            `mapstructure:"backfill"`
	RunAttribution            RunAttributionConfig      `mapstructure:"run_attribution"`
	Publisher                 *Publisher                `mapstructure:"publisher"`
	JobSyncIntervalMinutes    int                       `mapstructure:"job_sync_interval_minutes"`
	ExternalTables            ExternalTablesConfig      `mapstructure:"external_tables"`
	Features                  FeaturesConfig            `mapstructure:"features"`
	Plugins                   Plugins                   `mapstructure:"plugins"`
	JobValidationConfig       JobValidationConfig       `mapstructure:"job_validation"`
	JobExpectatorConfig       JobExpectatorConfig       `mapstructure:"job_expectator"`
	JobExecutionSummaryConfig JobExecutionSummaryConfig `mapstructure:"job_execution_summary"`
}

type UpstreamResolver struct {
	Type   UpstreamResolverType   `mapstructure:"type"`
	Config map[string]interface{} `mapstructure:"config"`
}

func (u UpstreamResolverType) String() string {
	return string(u)
}

func (u *UpstreamResolver) GetDexClientConfig() (*DexClientConfig, error) {
	configBytes, err := json.Marshal(u.Config)
	if err != nil {
		return nil, err
	}
	var dexClientConfig DexClientConfig
	err = json.Unmarshal(configBytes, &dexClientConfig)
	if err != nil {
		return nil, err
	}
	return &dexClientConfig, nil
}

type Serve struct {
	Port            int      `mapstructure:"port" default:"9100"` // port to listen on
	IngressHost     string   `mapstructure:"ingress_host"`        // service ingress host for jobs to communicate back to optimus
	PortGRPC        int      `mapstructure:"port_grpc"`
	IngressHostGRPC string   `mapstructure:"ingress_host_grpc"`
	AppKey          string   `mapstructure:"app_key"` // random 32 character hash used for encrypting secrets
	DB              DBConfig `mapstructure:"db"`
}

type DBConfig struct {
	DSN               string `mapstructure:"dsn"`                              // data source name e.g.: postgres://user:password@host:123/database?sslmode=disable
	MinOpenConnection int    `mapstructure:"min_open_connection" default:"5"`  // minimum open DB connections
	MaxOpenConnection int    `mapstructure:"max_open_connection" default:"20"` // maximum allowed open DB connections
}

type Plugins struct {
	Location string `mapstructure:"location"`
}

type TelemetryConfig struct {
	ProfileAddr      string `mapstructure:"profile_addr"`
	JaegerAddr       string `mapstructure:"jaeger_addr"`
	MetricServerAddr string `mapstructure:"telegraf_addr"`
}

type AlertingConfig struct {
	EventManager             EventManagerConfig       `mapstructure:"alert_manager"`
	Dashboard                string                   `mapstructure:"dashboard"`
	DataConsole              string                   `mapstructure:"data_console"`
	EnableSlack              bool                     `mapstructure:"enable_slack"`
	EnablePagerDuty          bool                     `mapstructure:"enable_pager_duty"`
	AutoSLABreachConfig      DurationEstimatorConfig  `mapstructure:"auto_sla_breach_config"`
	PotentialSLABreachConfig PotentialSLABreachConfig `mapstructure:"potential_sla_breach_config"`
}

type PotentialSLABreachConfig struct {
	DamperCoeff             float64                 `mapstructure:"damper_coeff" default:"1.0"`
	EnablePersistentLogging bool                    `mapstructure:"enable_persistent_logging" default:"false"`
	DurationEstimatorConfig DurationEstimatorConfig `mapstructure:"duration_estimator_config"`
}

type DurationEstimatorConfig struct {
	LastNRuns         int `mapstructure:"last_n_runs" default:"7"`
	Percentile        int `mapstructure:"percentile" default:"95"`
	PaddingPercentage int `mapstructure:"padding_percentage" default:"0"`
	MinPaddingMinutes int `mapstructure:"min_padding_minutes" default:"0"`
	MaxPaddingMinutes int `mapstructure:"max_padding_minutes" default:"1000"`
}

type SLAConfig struct {
	WorkerIntervalMinutes int `mapstructure:"worker_interval_minutes"`
	LockDurationMinutes   int `mapstructure:"worker_lock_duration_minutes"`
}

type ExternalTablesConfig struct {
	AccessIssuesRetryInterval int64 `mapstructure:"access_issues_retry_interval_minutes"`
	SourceSyncInterval        int64 `mapstructure:"source_sync_interval_minutes"`
	MaxFileSizeSupported      int   `mapstructure:"max_drive_file_size_mb"`
	DriveFileCleanupSizeLimit int   `mapstructure:"drive_file_cleanup_size_limit_mb"`
	MaxSyncDelayTolerance     int64 `mapstructure:"max_sync_delay_tolerance_hours"`
}

type EventManagerConfig struct {
	Host     string `mapstructure:"host"`
	Endpoint string `mapstructure:"endpoint"`
	Enabled  bool   `mapstructure:"enabled" default:"true"`
}

type ResourceManager struct {
	Name        string      `mapstructure:"name"`
	Type        string      `mapstructure:"type"`
	Description string      `mapstructure:"description"`
	Config      interface{} `mapstructure:"config"`
}

type ResourceManagerConfigOptimus struct {
	Host    string            `mapstructure:"host"`
	Headers map[string]string `mapstructure:"headers"`
}

type DexClientConfig struct {
	Host         string `mapstructure:"host" json:"host"`
	AuthEmail    string `mapstructure:"auth_email" json:"auth_email"`
	ProducerType string `mapstructure:"producer_type" json:"producer_type"`
}

type ReplayConfig struct {
	ReplayTimeoutInMinutes            int               `mapstructure:"replay_timeout_in_minutes" default:"180"`
	ExecutionIntervalInSeconds        int               `mapstructure:"execution_interval_in_seconds" default:"120"`
	PluginExecutionProjectConfigNames map[string]string `mapstructure:"plugin_execution_project_config_names"`
}

type BackfillConfig struct {
	ExecutionIntervalInSeconds int `mapstructure:"execution_interval_in_seconds" default:"300"`
}

// RunAttributionConfig governs how Optimus decides why a task or hook run is executing and
// who is answerable for it.
//
// The Airflow audit event names this relies on are not configurable: they are properties of a
// given Airflow version, established by reading its source, and a wrong value here would
// silently produce unattributed runs rather than an error.
type RunAttributionConfig struct {
	// AuditResolutionEnabled gates only the part that calls Airflow's audit log to find out who
	// triggered or cleared a run by hand. Off by default so the Airflow dependency can be turned
	// on per environment once the credential has `can_read on Audit Logs`.
	//
	// Attributing runs to Optimus's own replay and backfill requests is unaffected: that is a
	// local database lookup and is always active. With this off, a manual run is still recorded
	// as manual, only its actor is left unidentified.
	AuditResolutionEnabled bool `mapstructure:"audit_resolution_enabled" default:"false"`

	// ResolveTimeoutSeconds bounds one audit resolution, including its retries. The resolver
	// runs detached from the request that triggered it, so this is the only thing that ends it.
	ResolveTimeoutSeconds int `mapstructure:"resolve_timeout_seconds" default:"30"`
	// MaxConcurrentResolves caps in-flight resolutions. When saturated Optimus records the run
	// as manual with an unresolved actor rather than queueing work or blocking event ingestion.
	MaxConcurrentResolves int `mapstructure:"max_concurrent_resolves" default:"16"`
	ResolveRetryMax       int `mapstructure:"resolve_retry_max" default:"3"`
	ResolveRetryBackoffMs int `mapstructure:"resolve_retry_backoff_ms" default:"500"`

	// AuditLookbackMinutes is how far before a run's start time to search the audit log. It
	// must cover the delay between a user's click and the task actually starting, which
	// includes a scheduler loop plus queue and pool wait.
	AuditLookbackMinutes int `mapstructure:"audit_lookback_minutes" default:"30"`
	// AuditPageLimit caps rows fetched per audit query.
	AuditPageLimit int `mapstructure:"audit_page_limit" default:"100"`

	// ServiceAccountOwners are audit log owners that are Optimus itself. Optimus's own replay
	// and backfill calls into Airflow are audited too, so without this every replay would be
	// attributed to the service account. Deployment specific, hence configurable.
	ServiceAccountOwners []string `mapstructure:"service_account_owners"`
}

type Publisher struct {
	Type   string      `mapstructure:"type" default:"kafka"`
	Buffer int         `mapstructure:"buffer"`
	Config interface{} `mapstructure:"config"`
}

type PublisherKafkaConfig struct {
	Topic               string   `mapstructure:"topic"`
	BatchIntervalSecond int      `mapstructure:"batch_interval_second"`
	BrokerURLs          []string `mapstructure:"broker_urls"`
}

type FeaturesConfig struct {
	EnableV2Sensor                    bool `mapstructure:"enable_v2_sensor"`
	EnableV3Sensor                    bool `mapstructure:"enable_v3_sensor"`
	EnableIgnoreOldScheduleRunsSensor bool `mapstructure:"enable_ignore_old_schedule_runs_sensor"`
	EnableTableCommentWithMetadata    bool `mapstructure:"enable_table_comment_with_metadata"`
}

type JobValidationConfig struct {
	ValidateSchedule ValidateScheduleConfig `mapstructure:"validate_schedule"`
}

type ValidateScheduleConfig struct {
	ReferenceTimezone string `mapstructure:"reference_timezone"`
}

type JobExpectatorConfig struct {
	BufferDurationInMinutes int                     `mapstructure:"buffer_duration_in_minutes" default:"10"`
	DurationEstimatorConfig DurationEstimatorConfig `mapstructure:"duration_estimator_config"`
}

type JobExecutionSummaryConfig struct {
	MaxLineageDepth    int                      `mapstructure:"max_lineage_depth" default:"25"`
	HistoricalDuration HistoricalDurationConfig `mapstructure:"historical_duration"`
}

type HistoricalDurationConfig struct {
	LastNRuns  int `mapstructure:"last_n_runs" default:"7"`
	Percentile int `mapstructure:"percentile" default:"95"`
}
