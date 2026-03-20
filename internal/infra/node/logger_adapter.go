package node

import (
	"fmt"

	"go.etcd.io/etcd/raft/v3"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// loggerAdapter adapts the application logger to etcd/raft's Logger interface.
type loggerAdapter struct {
	logger logging.Logger
}

// NewLoggerAdapter creates a new logger adapter that implements raft.Logger.
func NewLoggerAdapter(logger logging.Logger) raft.Logger {
	return &loggerAdapter{logger: logger}
}

// Debug logs a debug message.
func (l *loggerAdapter) Debug(v ...any) {
	l.logger.Debugf("%s", fmt.Sprint(v...))
}

// Debugf logs a formatted debug message.
func (l *loggerAdapter) Debugf(format string, v ...any) {
	l.logger.Debugf(format, v...)
}

// Error logs an error message.
func (l *loggerAdapter) Error(v ...any) {
	l.logger.Errorf("%s", fmt.Sprint(v...))
}

// Errorf logs a formatted error message.
func (l *loggerAdapter) Errorf(format string, v ...any) {
	l.logger.Errorf(format, v...)
}

// Info logs an info message.
func (l *loggerAdapter) Info(v ...any) {
	l.logger.Infof("%s", fmt.Sprint(v...))
}

// Infof logs a formatted info message.
func (l *loggerAdapter) Infof(format string, v ...any) {
	l.logger.Infof(format, v...)
}

// Warning logs a warning message.
func (l *loggerAdapter) Warning(v ...any) {
	l.logger.Infof("WARN: %s", fmt.Sprint(v...))
}

// Warningf logs a formatted warning message.
func (l *loggerAdapter) Warningf(format string, v ...any) {
	l.logger.Infof("WARN: "+format, v...)
}

// Fatal logs a fatal message and exits.
func (l *loggerAdapter) Fatal(v ...any) {
	l.logger.Errorf("%s", fmt.Sprint(v...))
	panic(fmt.Sprint(v...))
}

// Fatalf logs a formatted fatal message and exits.
func (l *loggerAdapter) Fatalf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Errorf(msg)
	panic(msg)
}

// Panic logs a panic message and panics.
func (l *loggerAdapter) Panic(v ...any) {
	msg := fmt.Sprint(v...)
	l.logger.Errorf("%s", msg)
	panic(msg)
}

// Panicf logs a formatted panic message and panics.
func (l *loggerAdapter) Panicf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Errorf(msg)
	panic(msg)
}
