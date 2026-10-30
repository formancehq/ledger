package transport

import "context"

// PeerAuthMetadataKey carries the node-to-node transport credential. It is
// deliberately distinct from the standard authorization header, which is
// reserved for the original caller's user credential when a request is
// forwarded to another node.
const PeerAuthMetadataKey = "x-ledger-peer-auth"

type forwardedBearerTokenKey struct{}

// WithForwardedBearerToken records the caller's bearer token for the outgoing
// peer call. The connection pool's per-RPC credentials copy it into the
// standard authorization metadata without exposing the cluster credential as
// a user token.
func WithForwardedBearerToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}

	return context.WithValue(ctx, forwardedBearerTokenKey{}, token)
}

func ForwardedBearerToken(ctx context.Context) string {
	token, _ := ctx.Value(forwardedBearerTokenKey{}).(string)

	return token
}
