package logginglogrus

import (
	"context"

	"github.com/formancehq/go-libs/logging"
	"github.com/sirupsen/logrus"
)

type logrusLogger struct {
	entry interface {
		Debugf(format string, args ...any)
		Debug(args ...any)
		Infof(format string, args ...any)
		Info(args ...any)
		Errorf(format string, args ...any)
		Error(args ...any)
		WithFields(fields logrus.Fields) *logrus.Entry
		WithContext(ctx context.Context) *logrus.Entry
	}
}

func (l *logrusLogger) WithContext(ctx context.Context) logging.Logger {
	return &logrusLogger{
		l.entry.WithContext(ctx),
	}
}

func (l *logrusLogger) Debug(args ...any) {
	l.entry.Debug(args...)
}
func (l *logrusLogger) Debugf(fmt string, args ...any) {
	l.entry.Debugf(fmt, args...)
}
func (l *logrusLogger) Infof(fmt string, args ...any) {
	l.entry.Infof(fmt, args...)
}
func (l *logrusLogger) Info(args ...any) {
	l.entry.Info(args...)
}
func (l *logrusLogger) Errorf(fmt string, args ...any) {
	l.entry.Errorf(fmt, args...)
}
func (l *logrusLogger) Error(args ...any) {
	l.entry.Error(args...)
}
func (l *logrusLogger) WithFields(fields map[string]any) logging.Logger {
	return &logrusLogger{
		entry: l.entry.WithFields(fields),
	}
}

var _ logging.Logger = &logrusLogger{}

func New(logger *logrus.Logger) *logrusLogger {
	return &logrusLogger{
		entry: logger,
	}
}

func init() {
	logging.SetFactory(logging.StaticLoggerFactory(New(logrus.StandardLogger())))
}
