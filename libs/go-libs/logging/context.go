package logging

import "context"

type contextKey string

var loggerKey contextKey = "_logger"

func LoggerFromContext(ctx context.Context) Logger {
	l := ctx.Value(loggerKey)
	if l == nil {
		return &noOpLogger{}
	}
	return l.(Logger)
}

func ContextWithLogger(ctx context.Context, l Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}
