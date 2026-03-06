package otlplogs

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/formancehq/go-libs/v3/logging"
)

// ZapLogger adapts a *zap.SugaredLogger to the logging.Logger interface.
type ZapLogger struct {
	sugar *zap.SugaredLogger
}

var _ logging.Logger = (*ZapLogger)(nil)

// NewZapLogger wraps a *zap.SugaredLogger into a logging.Logger.
func NewZapLogger(sugar *zap.SugaredLogger) *ZapLogger {
	return &ZapLogger{sugar: sugar}
}

func (z *ZapLogger) Debugf(format string, args ...any) { z.sugar.Debugf(format, args...) }
func (z *ZapLogger) Infof(format string, args ...any)  { z.sugar.Infof(format, args...) }
func (z *ZapLogger) Errorf(format string, args ...any) { z.sugar.Errorf(format, args...) }
func (z *ZapLogger) Debug(args ...any)                 { z.sugar.Debug(args...) }
func (z *ZapLogger) Info(args ...any)                  { z.sugar.Info(args...) }
func (z *ZapLogger) Error(args ...any)                 { z.sugar.Error(args...) }

func (z *ZapLogger) WithFields(fields map[string]any) logging.Logger {
	kvs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		kvs = append(kvs, k, v)
	}

	return &ZapLogger{sugar: z.sugar.With(kvs...)}
}

func (z *ZapLogger) WithField(key string, value any) logging.Logger {
	return &ZapLogger{sugar: z.sugar.With(key, value)}
}

// WithContext returns self; OTel correlation is handled by the otelzap core bridge.
func (z *ZapLogger) WithContext(_ context.Context) logging.Logger {
	return z
}

// Writer returns an io.Writer that logs each line at Info level.
func (z *ZapLogger) Writer() io.Writer {
	pr, pw := io.Pipe()

	go func() {
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			z.sugar.Info(scanner.Text())
		}

		err := scanner.Err()
		if err != nil {
			z.sugar.Errorf("log writer scanner error: %v", err)
		}
	}()

	return pw
}

// Zap returns the underlying *zap.Logger for cases that require it (e.g. etcd WAL).
func (z *ZapLogger) Zap() *zap.Logger {
	return z.sugar.Desugar()
}

// NopLogger returns a silent logging.Logger backed by zap.NewNop().
func NopLogger() logging.Logger {
	return &ZapLogger{sugar: zap.NewNop().Sugar()}
}

// String implements fmt.Stringer for debug purposes.
func (z *ZapLogger) String() string {
	return fmt.Sprintf("ZapLogger{%v}", z.sugar)
}
