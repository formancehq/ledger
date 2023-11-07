package logging

import (
	"context"
	"flag"
	"io"
	"os"
	"testing"

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
		WithField(key string, value any) *logrus.Entry
		WithContext(ctx context.Context) *logrus.Entry
	}
}

func (l *logrusLogger) WithContext(ctx context.Context) Logger {
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
func (l *logrusLogger) WithFields(fields map[string]any) Logger {
	return &logrusLogger{
		entry: l.entry.WithFields(fields),
	}
}

func (l *logrusLogger) WithField(key string, value any) Logger {
	return l.WithFields(map[string]any{
		key: value,
	})
}

var _ Logger = &logrusLogger{}

func NewLogrus(logger *logrus.Logger) *logrusLogger {
	return &logrusLogger{
		entry: logger,
	}
}

func Testing() *logrusLogger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	flag.Parse()
	if testing.Verbose() {
		logger.SetOutput(os.Stdout)
		logger.SetLevel(logrus.DebugLevel)
	}

	textFormatter := new(logrus.TextFormatter)
	textFormatter.TimestampFormat = "15-01-2018 15:04:05.000000"
	textFormatter.FullTimestamp = true
	logger.SetFormatter(textFormatter)

	return NewLogrus(logger)
}
