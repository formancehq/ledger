package logging

// https://github.com/garsue/watermillzap/blob/master/adapter.go
import (
	"github.com/ThreeDotsLabs/watermill"
	"go.uber.org/zap"
)

// Logger implements watermill.LoggerAdapter with *zap.Logger.
type ZapLoggerAdapter struct {
	backend *zap.Logger
	fields  watermill.LogFields
}

// NewLogger returns new watermill.LoggerAdapter using passed *zap.Logger as backend.
func NewZapLoggerAdapter(z *zap.Logger) watermill.LoggerAdapter {
	return &ZapLoggerAdapter{backend: z}
}

// Error writes error log with message, error and some fields.
func (l *ZapLoggerAdapter) Error(msg string, err error, fields watermill.LogFields) {
	fields = l.fields.Add(fields)
	fs := make([]zap.Field, 0, len(fields)+1)
	fs = append(fs, zap.Error(err))
	for k, v := range fields {
		fs = append(fs, zap.Any(k, v))
	}
	l.backend.Error(msg, fs...)
}

// Info writes info log with message and some fields.
func (l *ZapLoggerAdapter) Info(msg string, fields watermill.LogFields) {
	fields = l.fields.Add(fields)
	fs := make([]zap.Field, 0, len(fields)+1)
	for k, v := range fields {
		fs = append(fs, zap.Any(k, v))
	}
	l.backend.Info(msg, fs...)
}

// Debug writes debug log with message and some fields.
func (l *ZapLoggerAdapter) Debug(msg string, fields watermill.LogFields) {
	fields = l.fields.Add(fields)
	fs := make([]zap.Field, 0, len(fields)+1)
	for k, v := range fields {
		fs = append(fs, zap.Any(k, v))
	}
	l.backend.Debug(msg, fs...)
}

// Trace writes debug log instead of trace log because zap does not support trace level logging.
func (l *ZapLoggerAdapter) Trace(msg string, fields watermill.LogFields) {
	fields = l.fields.Add(fields)
	fs := make([]zap.Field, 0, len(fields)+1)
	for k, v := range fields {
		fs = append(fs, zap.Any(k, v))
	}
	l.backend.Debug(msg, fs...)
}

// With returns new LoggerAdapter with passed fields.
func (l *ZapLoggerAdapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &ZapLoggerAdapter{
		backend: l.backend,
		fields:  l.fields.Add(fields),
	}
}
