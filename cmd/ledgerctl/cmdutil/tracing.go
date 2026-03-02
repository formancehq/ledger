package cmdutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// traceID is generated once at init time so every RPC from this CLI
// invocation shares the same W3C trace ID in server-side spans.
var traceID = generateTraceID()

func generateTraceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func generateSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func traceparent() string {
	return fmt.Sprintf("00-%s-%s-01", traceID, generateSpanID())
}

// TracingUnaryInterceptor injects a W3C traceparent header into every unary RPC.
func TracingUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "traceparent", traceparent())
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// TracingStreamInterceptor injects a W3C traceparent header into every streaming RPC.
func TracingStreamInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "traceparent", traceparent())
		return streamer(ctx, desc, cc, method, opts...)
	}
}
