package writer

import (
	"errors"
	"sync"

	"github.com/goto/salt/log"

	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type saltLogger struct {
	l log.Logger
}

func NewLogWriter(l log.Logger) LogWriter {
	return &saltLogger{
		l: l,
	}
}

func (l *saltLogger) Write(level LogLevel, message string) error {
	switch level {
	case LogLevelTrace:
		l.l.Debug(message)
	case LogLevelDebug:
		l.l.Debug(message)
	case LogLevelInfo:
		l.l.Info(message)
	case LogLevelWarning:
		l.l.Warn(message)
	case LogLevelError:
		l.l.Error(message)
	case LogLevelFatal:
		l.l.Fatal(message)
	}
	return nil
}

type BufferedLogger struct {
	Messages []*pb.Log

	mtx *sync.Mutex
}

func NewSafeBufferedLogger() *BufferedLogger {
	return &BufferedLogger{
		Messages: nil,
		mtx:      new(sync.Mutex),
	}
}

// nolint: unparam
func (b *BufferedLogger) Write(level LogLevel, message string) error {
	if b == nil {
		return errors.New("buffered logger is nil")
	}

	if b.mtx == nil {
		b.mtx = new(sync.Mutex)
	}

	b.mtx.Lock()
	b.Messages = append(b.Messages, newLogStatusProto(level, message))
	b.mtx.Unlock()

	return nil
}
