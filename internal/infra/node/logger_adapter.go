package node

import (
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/raft/v3"
)

// loggerAdapter adapts the application logger to etcd/raft's Logger interface
type loggerAdapter struct {
	logger logging.Logger
}

// NewLoggerAdapter creates a new logger adapter that implements raft.Logger
func NewLoggerAdapter(logger logging.Logger) raft.Logger {
	return &loggerAdapter{logger: logger}
}

// Debug logs a debug message
func (l *loggerAdapter) Debug(v ...interface{}) {
	l.logger.Debugf("%s", fmt.Sprint(v...))
}

// Debugf logs a formatted debug message
func (l *loggerAdapter) Debugf(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}

// Error logs an error message
func (l *loggerAdapter) Error(v ...interface{}) {
	l.logger.Errorf("%s", fmt.Sprint(v...))
}

// Errorf logs a formatted error message
func (l *loggerAdapter) Errorf(format string, v ...interface{}) {
	l.logger.Errorf(format, v...)
}

// Info logs an info message
func (l *loggerAdapter) Info(v ...interface{}) {
	l.logger.Infof("%s", fmt.Sprint(v...))
}

// Infof logs a formatted info message
func (l *loggerAdapter) Infof(format string, v ...interface{}) {
	l.logger.Infof(format, v...)
}

// Warning logs a warning message
func (l *loggerAdapter) Warning(v ...interface{}) {
	l.logger.Infof("WARN: %s", fmt.Sprint(v...))
}

// Warningf logs a formatted warning message
func (l *loggerAdapter) Warningf(format string, v ...interface{}) {
	l.logger.Infof("WARN: "+format, v...)
}

// Fatal logs a fatal message and exits
func (l *loggerAdapter) Fatal(v ...interface{}) {
	l.logger.Errorf("%s", fmt.Sprint(v...))
	panic(fmt.Sprint(v...))
}

// Fatalf logs a formatted fatal message and exits
func (l *loggerAdapter) Fatalf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Errorf(msg)
	panic(msg)
}

// Panic logs a panic message and panics
func (l *loggerAdapter) Panic(v ...interface{}) {
	msg := fmt.Sprint(v...)
	l.logger.Errorf("%s", msg)
	panic(msg)
}

// Panicf logs a formatted panic message and panics
func (l *loggerAdapter) Panicf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Errorf(msg)
	panic(msg)
}
