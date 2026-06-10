package grpc

import (
	"context"
	"crypto/subtle"
	"strings"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// raftAuthInterceptors returns the unary and stream interceptors that guard
// the Raft gRPC plane. Every RPC must carry an `authorization: Bearer
// <clusterSecret>` metadata pair; mismatches are rejected with
// codes.Unauthenticated.
//
// When clusterSecret is empty, the interceptors are nil — the server is
// unauthenticated, which matches the historical behavior for clusters that
// never opted into a shared secret. Operators wanting to lock the plane down
// MUST set --cluster-secret (and --tls-mode, already enforced by
// bootstrap.Config.Validate).
//
// The token comparison uses crypto/subtle.ConstantTimeCompare so a network
// attacker cannot byte-by-byte time the secret out of the server.
func raftAuthInterceptors(clusterSecret string) (ggrpc.UnaryServerInterceptor, ggrpc.StreamServerInterceptor) {
	if clusterSecret == "" {
		return nil, nil
	}

	expected := []byte(clusterSecret)

	checkToken := func(ctx context.Context) error {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata on Raft RPC")
		}

		values := md.Get("authorization")
		if len(values) == 0 {
			return status.Error(codes.Unauthenticated, "missing authorization metadata on Raft RPC")
		}

		header := values[0]
		if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
			return status.Error(codes.Unauthenticated, "authorization header must be a Bearer token on Raft RPC")
		}

		got := strings.TrimSpace(header[len("Bearer "):])
		if subtle.ConstantTimeCompare([]byte(got), expected) != 1 {
			return status.Error(codes.Unauthenticated, "invalid cluster credentials on Raft RPC")
		}

		return nil
	}

	unary := func(ctx context.Context, req any, _ *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		if err := checkToken(ctx); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}

	stream := func(srv any, ss ggrpc.ServerStream, _ *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		if err := checkToken(ss.Context()); err != nil {
			return err
		}

		return handler(srv, ss)
	}

	return unary, stream
}
