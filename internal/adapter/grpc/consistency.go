package grpc

import (
	"context"
	"strings"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Consistency levels for read operations.
const (
	// ConsistencyLinearizable is the default: ReadIndex barrier on the local node.
	ConsistencyLinearizable = "linearizable"
	// ConsistencyStale skips the ReadIndex barrier and reads from the local store directly.
	// Data may lag behind the latest committed index.
	ConsistencyStale = "stale"
	// ConsistencyLeader forwards the read to the leader node, which always has
	// the most up-to-date data and a fast ReadIndex barrier.
	ConsistencyLeader = "leader"
)

const metadataKeyConsistency = "x-consistency"

type consistencyKey struct{}

// WithConsistency returns a copy of ctx with the given consistency level stored.
func WithConsistency(ctx context.Context, level string) context.Context {
	return context.WithValue(ctx, consistencyKey{}, level)
}

// ConsistencyFromContext returns the consistency level from ctx, defaulting to linearizable.
func ConsistencyFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(consistencyKey{}).(string); ok && v != "" {
		return v
	}

	return ConsistencyLinearizable
}

// IsStaleRead returns true if the context carries a stale consistency level.
func IsStaleRead(ctx context.Context) bool {
	return ConsistencyFromContext(ctx) == ConsistencyStale
}

// IsLeaderRead returns true if the context carries a leader consistency level.
func IsLeaderRead(ctx context.Context) bool {
	return ConsistencyFromContext(ctx) == ConsistencyLeader
}

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
// (defaults to linearizable).
func extractConsistency(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	vals := md.Get(metadataKeyConsistency)
	if len(vals) == 0 {
		return ctx
	}

	level := strings.ToLower(strings.TrimSpace(vals[0]))
	switch level {
	case ConsistencyStale, ConsistencyLeader:
		return WithConsistency(ctx, level)
	default:
		return ctx
	}
}
