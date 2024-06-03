package config

type ServerConfig struct {
	Version          Version           `mapstructure:"version"`
	Log              LogConfig         `mapstructure:"log"`
	Serve            Serve             `mapstructure:"serve"`
	Telemetry        TelemetryConfig   `mapstructure:"telemetry"`
	Alerting         AlertingConfig    `mapstructure:"alerting"`
	ResourceManagers []ResourceManager `mapstructure:"resource_managers"`
	Plugin           PluginConfig      `mapstructure:"plugin"`
	Replay           ReplayConfig      `mapstructure:"replay"`
	Publisher        *Publisher        `mapstructure:"publisher"`
}

type Serve struct {
	Port            int      `default:"9100"                   mapstructure:"port"` // port to listen on
	IngressHost     string   `mapstructure:"ingress_host"`                          // service ingress host for jobs to communicate back to optimus
	PortGRPC        int      `mapstructure:"port_grpc"`
	IngressHostGRPC string   `mapstructure:"ingress_host_grpc"`
	AppKey          string   `mapstructure:"app_key"` // random 32 character hash used for encrypting secrets
	DB              DBConfig `mapstructure:"db"`
}

type DBConfig struct {
	DSN               string `mapstructure:"dsn"`                                    // data source name e.g.: postgres://user:password@host:123/database?sslmode=disable
	MinOpenConnection int    `default:"5"        mapstructure:"min_open_connection"` // minimum open DB connections
	MaxOpenConnection int    `default:"20"       mapstructure:"max_open_connection"` // maximum allowed open DB connections
}

type TelemetryConfig struct {
	ProfileAddr      string `mapstructure:"profile_addr"`
	JaegerAddr       string `mapstructure:"jaeger_addr"`
	MetricServerAddr string `mapstructure:"telegraf_addr"`
}

type AlertingConfig struct {
	EventManager EventManagerConfig `mapstructure:"alert_manager"`
	Dashboard    string             `mapstructure:"dashboard"`
	DataConsole  string             `mapstructure:"data_console"`
}

type EventManagerConfig struct {
	Host     string `mapstructure:"host"`
	Endpoint string `mapstructure:"endpoint"`
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

type PluginConfig struct {
	Artifacts []string `mapstructure:"artifacts"`
}

type ReplayConfig struct {
	ReplayTimeoutInMinutes            int               `default:"180"                                        mapstructure:"replay_timeout_in_minutes"`
	ExecutionIntervalInSeconds        int               `default:"120"                                        mapstructure:"execution_interval_in_seconds"`
	PluginExecutionProjectConfigNames map[string]string `mapstructure:"plugin_execution_project_config_names"`
}

type Publisher struct {
	Type   string      `default:"kafka"       mapstructure:"type"`
	Buffer int         `mapstructure:"buffer"`
	Config interface{} `mapstructure:"config"`
}

type PublisherKafkaConfig struct {
	Topic               string   `mapstructure:"topic"`
	BatchIntervalSecond int      `mapstructure:"batch_interval_second"`
	BrokerURLs          []string `mapstructure:"broker_urls"`
}
