package config

type ServerConfig struct {
	Version                Version              `mapstructure:"version"`
	Log                    LogConfig            `mapstructure:"log"`
	Serve                  Serve                `mapstructure:"serve"`
	Telemetry              TelemetryConfig      `mapstructure:"telemetry"`
	Alerting               AlertingConfig       `mapstructure:"alerting"`
	ResourceManagers       []ResourceManager    `mapstructure:"resource_managers"`
	Replay                 ReplayConfig         `mapstructure:"replay"`
	Publisher              *Publisher           `mapstructure:"publisher"`
	JobSyncIntervalMinutes int                  `mapstructure:"job_sync_interval_minutes"`
	ExternalTables         ExternalTablesConfig `mapstructure:"external_tables"`
	Features               FeaturesConfig       `mapstructure:"features"`
	Plugins                Plugins              `mapstructure:"plugins"`
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
	EventManager    EventManagerConfig `mapstructure:"alert_manager"`
	Dashboard       string             `mapstructure:"dashboard"`
	DataConsole     string             `mapstructure:"data_console"`
	EnableSlack     bool               `mapstructure:"enable_slack"`
	EnablePagerDuty bool               `mapstructure:"enable_pager_duty"`
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

type ReplayConfig struct {
	ReplayTimeoutInMinutes            int               `mapstructure:"replay_timeout_in_minutes" default:"180"`
	ExecutionIntervalInSeconds        int               `mapstructure:"execution_interval_in_seconds" default:"120"`
	PluginExecutionProjectConfigNames map[string]string `mapstructure:"plugin_execution_project_config_names"`
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
	EnableV3Sensor bool `mapstructure:"enable_v3_sensor"`
}
