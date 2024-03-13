package grpc

// I want to create a logging middle for grpc

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"google.golang.org/grpc"
)

// UnaryServerLoggingInterceptor returns a new unary server interceptor that logs the request and response.
func UnaryServerLoggingInterceptor(logger logging.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx = logging.ContextWithLogger(ctx, logger)
		logging.FromContext(ctx).Debugf("unary server interceptor: %s", info.FullMethod)
		logging.FromContext(ctx).Debugf("request: %v", req)
		resp, err := handler(ctx, req)
		if err != nil {
			logging.FromContext(ctx).Errorf("error: %v", err)
		}
		logging.FromContext(ctx).Debugf("response: %v", resp)
		return resp, err
	}
}
