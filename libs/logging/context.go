package logging

import (
	"context"

	"github.com/sirupsen/logrus"
)

type contextKey string

var loggerKey contextKey = "_logger"

func FromContext(ctx context.Context) Logger {
	l := ctx.Value(loggerKey)
	if l == nil {
		return NewLogrus(logrus.New())
	}
	return l.(Logger)
}

func ContextWithLogger(ctx context.Context, l Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

func ContextWithFields(ctx context.Context, fields map[string]any) context.Context {
	return ContextWithLogger(ctx, FromContext(ctx).WithFields(fields))
}

func ContextWithField(ctx context.Context, key string, value any) context.Context {
	return ContextWithLogger(ctx, FromContext(ctx).WithFields(map[string]any{
		key: value,
	}))
}

func TestingContext() context.Context {
	return ContextWithLogger(context.Background(), Testing())
}
