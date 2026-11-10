package grpc

import (
	"context"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// peerUsesTLS identifies the authenticated transport, not a client-provided
// header. Optional-mode plaintext peers are tolerated only during migration.
func peerUsesTLS(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	_, ok = p.AuthInfo.(credentials.TLSInfo)
	return ok
}
