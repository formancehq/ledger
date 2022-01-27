package logging

import (
	"context"
	"github.com/sirupsen/logrus"
)

type Logger interface {
	Info(ctx context.Context, fmt string, args ...interface{})
	Error(ctx context.Context, fmt string, args ...interface{})
	Warn(ctx context.Context, fmt string, args ...interface{})
	Debug(ctx context.Context, fmt string, args ...interface{})
	WithFields(map[string]interface{}) Logger
}

type logrusLogger struct {
	*logrus.Entry
}

var defaultLogger Logger = &logrusLogger{
	Entry: logrus.NewEntry(logrus.StandardLogger()),
}

func SetDefaultLogger(l Logger) {
	defaultLogger = l
}

func DefaultLogger() Logger {
	return defaultLogger
}

func Info(ctx context.Context, fmt string, args ...interface{}) {
	defaultLogger.Info(ctx, fmt, args...)
}
func Error(ctx context.Context, fmt string, args ...interface{}) {
	defaultLogger.Error(ctx, fmt, args...)
}
func Warn(ctx context.Context, fmt string, args ...interface{}) {
	defaultLogger.Warn(ctx, fmt, args...)
}
func Debug(ctx context.Context, fmt string, args ...interface{}) {
	defaultLogger.Debug(ctx, fmt, args...)
}
func WithFields(fields map[string]interface{}) Logger {
	return defaultLogger.WithFields(fields)
}

const (
	LevelInfo  = "info"
	LevelError = "error"
	LevelWarn  = "warn"
	LevelDebug = "debug"
)

type entry struct {
	Fields map[string]interface{}
	Fmt    string
	Args   []interface{}
	Level  string
}

type inMemoryLogger struct {
	entries    []entry
	fields     map[string]interface{}
	subLoggers []*inMemoryLogger
}

func (l *inMemoryLogger) appendEntry(level string, fmt string, args ...interface{}) {
	l.entries = append(l.entries, entry{
		Fields: l.fields,
		Fmt:    fmt,
		Args:   args,
		Level:  level,
	})
}

func (l *inMemoryLogger) Info(ctx context.Context, fmt string, args ...interface{}) {
	l.appendEntry(LevelInfo, fmt, args...)
}
func (l *inMemoryLogger) Error(ctx context.Context, fmt string, args ...interface{}) {
	l.appendEntry(LevelError, fmt, args...)
}
func (l *inMemoryLogger) Warn(ctx context.Context, fmt string, args ...interface{}) {
	l.appendEntry(LevelWarn, fmt, args...)
}
func (l *inMemoryLogger) Debug(ctx context.Context, fmt string, args ...interface{}) {
	l.appendEntry(LevelDebug, fmt, args...)
}
func (l *inMemoryLogger) WithFields(fields map[string]interface{}) Logger {
	allFields := make(map[string]interface{})
	if l.fields != nil {
		for key, value := range l.fields {
			allFields[key] = value
		}
	}
	for key, value := range fields {
		allFields[key] = value
	}
	ret := &inMemoryLogger{
		entries: make([]entry, 0),
		fields:  allFields,
	}
	l.subLoggers = append(l.subLoggers, ret)
	return ret
}

func (l *inMemoryLogger) Entries() []entry {
	return l.entries
}

func (l *inMemoryLogger) SubLoggers() []*inMemoryLogger {
	return l.subLoggers
}

func NewInMemoryLogger() *inMemoryLogger {
	return &inMemoryLogger{
		entries: make([]entry, 0),
		fields:  map[string]interface{}{},
	}
}
