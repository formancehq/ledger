package contextlogger

import (
	"context"

	"github.com/formancehq/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg"
)

var _ sharedlogging.Logger = &ContextLogger{}

type ContextLogger struct {
	ctx              context.Context
	underlyingLogger sharedlogging.Logger
}

func New(ctx context.Context, logger sharedlogging.Logger) *ContextLogger {
	return &ContextLogger{
		ctx:              ctx,
		underlyingLogger: logger,
	}
}

func (c ContextLogger) Debugf(format string, args ...any) {
	id := c.ctx.Value(pkg.KeyContextID)
	c.underlyingLogger.
		WithFields(map[string]any{string(pkg.KeyContextID): id}).
		Debugf(format, args...)
}

func (c ContextLogger) Infof(format string, args ...any) {
	id := c.ctx.Value(pkg.KeyContextID)
	c.underlyingLogger.
		WithFields(map[string]any{string(pkg.KeyContextID): id}).
		Infof(format, args...)
}

func (c ContextLogger) Errorf(format string, args ...any) {
	id := c.ctx.Value(pkg.KeyContextID)
	c.underlyingLogger.
		WithFields(map[string]any{string(pkg.KeyContextID): id}).
		Errorf(format, args...)
}

func (c ContextLogger) Debug(args ...any) {
	c.underlyingLogger.Debug(args...)
}

func (c ContextLogger) Info(args ...any) {
	c.underlyingLogger.Info(args...)
}

func (c ContextLogger) Error(args ...any) {
	c.underlyingLogger.Error(args...)
}

func (c ContextLogger) WithFields(m map[string]any) sharedlogging.Logger {
	m[string(pkg.KeyContextID)] = c.ctx.Value(pkg.KeyContextID)
	return &ContextLogger{
		ctx:              c.ctx,
		underlyingLogger: c.underlyingLogger.WithFields(m),
	}
}

func (c ContextLogger) WithContext(ctx context.Context) sharedlogging.Logger {
	return &ContextLogger{
		ctx:              ctx,
		underlyingLogger: c.underlyingLogger,
	}
}
