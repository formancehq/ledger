package logging

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

type LogrusLogger struct {
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
		Writer() *io.PipeWriter
	}
}

func (l *LogrusLogger) Writer() io.Writer {
	return l.entry.Writer()
}

func (l *LogrusLogger) WithContext(ctx context.Context) Logger {
	return &LogrusLogger{
		l.entry.WithContext(ctx),
	}
}

func (l *LogrusLogger) Debug(args ...any) {
	l.entry.Debug(args...)
}
func (l *LogrusLogger) Debugf(fmt string, args ...any) {
	l.entry.Debugf(fmt, args...)
}
func (l *LogrusLogger) Infof(fmt string, args ...any) {
	l.entry.Infof(fmt, args...)
}
func (l *LogrusLogger) Info(args ...any) {
	l.entry.Info(args...)
}
func (l *LogrusLogger) Errorf(fmt string, args ...any) {
	l.entry.Errorf(fmt, args...)
}
func (l *LogrusLogger) Error(args ...any) {
	l.entry.Error(args...)
}
func (l *LogrusLogger) WithFields(fields map[string]any) Logger {
	return &LogrusLogger{
		entry: l.entry.WithFields(fields),
	}
}

func (l *LogrusLogger) WithField(key string, value any) Logger {
	return l.WithFields(map[string]any{
		key: value,
	})
}

var _ Logger = &LogrusLogger{}

func NewLogrus(logger *logrus.Logger) *LogrusLogger {
	return &LogrusLogger{
		entry: logger,
	}
}

func Testing() *LogrusLogger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	if testing.Verbose() {
		logger.SetOutput(os.Stderr)
		logger.SetLevel(logrus.DebugLevel)
	}

	textFormatter := new(logrus.TextFormatter)
	textFormatter.TimestampFormat = "15-01-2018 15:04:05.000000"
	textFormatter.FullTimestamp = true
	logger.SetFormatter(textFormatter)

	return NewLogrus(logger)
}

func NewDefaultLogger(w io.Writer, debug, formatJSON bool, hooks ...logrus.Hook) *LogrusLogger {
	l := logrus.New()
	l.SetOutput(w)
	if debug {
		l.Level = logrus.DebugLevel
	}

	var formatter logrus.Formatter
	if formatJSON {
		jsonFormatter := &logrus.JSONFormatter{}
		formatter = jsonFormatter
	} else {
		textFormatter := new(logrus.TextFormatter)
		textFormatter.FullTimestamp = true
		formatter = textFormatter
	}

	l.SetFormatter(formatter)

	for _, hook := range hooks {
		l.AddHook(hook)
	}

	return NewLogrus(l)
}
