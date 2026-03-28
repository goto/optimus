package larkdrive

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

type Logger struct {
	log.Logger
}

func (l *Logger) toMessage(i ...interface{}) string {
	return fmt.Sprintf("%v", i...)
}

func (l *Logger) Debug(_ context.Context, i ...interface{}) {
	l.Logger.Debug(l.toMessage(i...))
}

func (l *Logger) Info(_ context.Context, i ...interface{}) {
	l.Logger.Info(l.toMessage(i...))
}

func (l *Logger) Warn(_ context.Context, i ...interface{}) {
	l.Logger.Warn(l.toMessage(i...))
}

func (l *Logger) Error(_ context.Context, i ...interface{}) {
	l.Logger.Error(l.toMessage(i...))
}

func NewLogger(logger log.Logger) larkcore.Logger {
	return &Logger{Logger: logger}
}
