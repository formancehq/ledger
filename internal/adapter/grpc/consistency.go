package grpc

import (
	"context"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/query"
)

const metadataKeyConsistency = "x-consistency"

// consistencyInterceptor reads x-consistency from incoming gRPC metadata
// and stores the value in context for downstream handlers.
func consistencyInterceptor() ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		ctx = extractConsistency(ctx)

		return handler(ctx, req)
	}
}

// consistencyStreamInterceptor reads x-consistency from incoming gRPC metadata
// and stores the value in context for downstream streaming handlers.
func consistencyStreamInterceptor() ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		ctx := extractConsistency(ss.Context())

		return handler(srv, &consistencyServerStream{ServerStream: ss, ctx: ctx})
	}
}

// consistencyServerStream wraps a ServerStream to override its Context.
type consistencyServerStream struct {
	ggrpc.ServerStream

	ctx context.Context
}

func (s *consistencyServerStream) Context() context.Context {
	return s.ctx
}

// extractConsistency reads x-consistency from incoming gRPC metadata and returns
// a context with the consistency level set. Unrecognised values are ignored
// (defaults to linearizable); only the first metadata value is considered.
func extractConsistency(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	vals := md.Get(metadataKeyConsistency)
	if len(vals) == 0 {
		return ctx
	}

	if level, ok := query.ParseConsistency(vals[0]); ok && level == query.ConsistencyStale {
		return query.WithConsistency(ctx, level)
	}

	return ctx
}
