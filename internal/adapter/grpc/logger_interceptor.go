package grpc

import (
	"context"

	ggrpc "google.golang.org/grpc"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// loggerInterceptor injects the configured logger into every unary call
// context so downstream code (auth failure logging, error sanitization) can
// resolve it via logging.FromContext. Without this, FromContext degrades to
// go-libs' bare logrus stderr fallback and emits lines in a different format
// than the rest of the process. This mirrors the HTTP loggerMiddleware.
func loggerInterceptor(logger logging.Logger) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		return handler(logging.ContextWithLogger(ctx, logger), req)
	}
}

// loggerStreamInterceptor is the streaming counterpart of loggerInterceptor.
func loggerStreamInterceptor(logger logging.Logger) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		ctx := logging.ContextWithLogger(ss.Context(), logger)

		return handler(srv, &loggerServerStream{ServerStream: ss, ctx: ctx})
	}
}

// loggerServerStream wraps a ServerStream to override its Context.
type loggerServerStream struct {
	ggrpc.ServerStream

	ctx context.Context
}

func (s *loggerServerStream) Context() context.Context {
	return s.ctx
}
