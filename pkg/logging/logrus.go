package logging

import (
	"context"
	"github.com/sirupsen/logrus"
	"go.uber.org/fx"
)

func (l *logrusLogger) Info(ctx context.Context, fmt string, args ...interface{}) {
	l.Entry.WithContext(ctx).Infof(fmt, args...)
}
func (l *logrusLogger) Error(ctx context.Context, fmt string, args ...interface{}) {
	l.Entry.WithContext(ctx).Errorf(fmt, args...)
}
func (l *logrusLogger) Warn(ctx context.Context, fmt string, args ...interface{}) {
	l.Entry.WithContext(ctx).Warnf(fmt, args...)
}
func (l *logrusLogger) Debug(ctx context.Context, fmt string, args ...interface{}) {
	l.Entry.WithContext(ctx).Debugf(fmt, args...)
}
func (l *logrusLogger) WithFields(fields map[string]interface{}) Logger {
	return &logrusLogger{l.Entry.WithFields(fields)}
}

func NewLogrusLogger() *logrusLogger {
	return &logrusLogger{
		Entry: logrus.NewEntry(logrus.New()),
	}
}

func LogrusModule() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(NewLogrusLogger, fx.As(new(Logger)))),
	)
}
