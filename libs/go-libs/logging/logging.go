package logging

import "context"

type Logger interface {
	Debugf(fmt string, args ...any)
	Infof(fmt string, args ...any)
	Errorf(fmt string, args ...any)
	Debug(args ...any)
	Info(args ...any)
	Error(args ...any)
	WithFields(map[string]any) Logger
	WithContext(ctx context.Context) Logger
}

type LoggerFactory interface {
	Get(ctx context.Context) Logger
}
type LoggerFactoryFn func(ctx context.Context) Logger

func (fn LoggerFactoryFn) Get(ctx context.Context) Logger {
	return fn(ctx)
}

func StaticLoggerFactory(l Logger) LoggerFactoryFn {
	return func(ctx context.Context) Logger {
		return l
	}
}

type noOpLogger struct{}

func (n noOpLogger) WithContext(ctx context.Context) Logger {
	return n
}

func (n noOpLogger) Debug(args ...any)              {}
func (n noOpLogger) Info(args ...any)               {}
func (n noOpLogger) Error(args ...any)              {}
func (n noOpLogger) Debugf(fmt string, args ...any) {}
func (n noOpLogger) Infof(fmt string, args ...any)  {}
func (n noOpLogger) Errorf(fmt string, args ...any) {}
func (n noOpLogger) WithFields(m map[string]any) Logger {
	return n
}

var _ Logger = &noOpLogger{}

func NewNoOpLogger() *noOpLogger {
	return &noOpLogger{}
}

var loggerFactory LoggerFactory

func SetFactory(l LoggerFactory) {
	loggerFactory = l
}

func GetLogger(ctx context.Context) Logger {
	if loggerFactory == nil {
		return NewNoOpLogger()
	}
	return loggerFactory.Get(ctx)
}

func Debugf(fmt string, args ...any) {
	GetLogger(context.Background()).Debugf(fmt, args...)
}
func Infof(fmt string, args ...any) {
	GetLogger(context.Background()).Infof(fmt, args...)
}
func Errorf(fmt string, args ...any) {
	GetLogger(context.Background()).Errorf(fmt, args...)
}
func Debug(args ...any) {
	GetLogger(context.Background()).Debug(args...)
}
func Info(args ...any) {
	GetLogger(context.Background()).Info(args...)
}
func Error(args ...any) {
	GetLogger(context.Background()).Error(args...)
}
func WithFields(v map[string]any) Logger {
	return GetLogger(context.Background()).WithFields(v)
}
