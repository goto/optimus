package config

import "strconv"

// Contains shared config for server and client (project)

// Config is just an alias for interface{}
type Config interface{}

// Version implement fmt.Stringer
type Version int

const DefaultVersion Version = 1

type LogLevel string

const (
	LogLevelDebug   LogLevel = "DEBUG"
	LogLevelInfo    LogLevel = "INFO"
	LogLevelWarning LogLevel = "WARNING"
	LogLevelError   LogLevel = "ERROR"
	LogLevelFatal   LogLevel = "FATAL"
)

type LogConfig struct {
	Level  LogLevel `default:"INFO"        mapstructure:"level"` // log level - debug, info, warning, error, fatal
	Format string   `mapstructure:"format"`                      // format strategy - plain, json
}

func (v Version) String() string {
	return strconv.Itoa(int(v))
}

func (l LogLevel) String() string {
	return string(l)
}
