package contextlogger

import (
	"context"

	"github.com/formancehq/go-libs/logging"
	"github.com/numary/ledger/pkg"
)

var _ logging.Logger = &ContextLogger{}

type ContextLogger struct {
	ctx              context.Context
	underlyingLogger logging.Logger
}

func New(ctx context.Context, logger logging.Logger) *ContextLogger {
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

func (c ContextLogger) WithFields(m map[string]any) logging.Logger {
	m[string(pkg.KeyContextID)] = c.ctx.Value(pkg.KeyContextID)
	return &ContextLogger{
		ctx:              c.ctx,
		underlyingLogger: c.underlyingLogger.WithFields(m),
	}
}

func (c ContextLogger) WithContext(ctx context.Context) logging.Logger {
	return &ContextLogger{
		ctx:              ctx,
		underlyingLogger: c.underlyingLogger,
	}
}
