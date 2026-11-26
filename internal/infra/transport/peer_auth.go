package transport

import "context"

// PeerAuthMetadataKey carries the node-to-node transport credential. It is
// deliberately distinct from the standard authorization header, which is
// reserved for the original caller's user credential when a request is
// forwarded to another node.
const PeerAuthMetadataKey = "x-ledger-peer-auth"

// ForwardedAnonymousMetadataKey identifies a caller that entered another
// node without a bearer token. Only the authenticated peer listener may use
// it to distinguish that caller from a node's own internal work.
const ForwardedAnonymousMetadataKey = "x-ledger-forwarded-anonymous"

type forwardedCallerKey struct{}

type forwardedCaller struct {
	bearerToken string
	anonymous   bool
}

// WithForwardedBearerToken records the caller's bearer token for the outgoing
// peer call. The connection pool's per-RPC credentials copy it into the
// standard authorization metadata without exposing the cluster credential as
// a user token.
func WithForwardedBearerToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}

	return context.WithValue(ctx, forwardedCallerKey{}, forwardedCaller{bearerToken: token})
}

func ForwardedBearerToken(ctx context.Context) string {
	caller, _ := ctx.Value(forwardedCallerKey{}).(forwardedCaller)

	return caller.bearerToken
}

// WithForwardedAnonymousCaller records that an authenticated ingress request
// had no bearer token. The peer listener re-evaluates anonymous scopes locally.
func WithForwardedAnonymousCaller(ctx context.Context) context.Context {
	return context.WithValue(ctx, forwardedCallerKey{}, forwardedCaller{anonymous: true})
}

func ForwardedAnonymousCaller(ctx context.Context) bool {
	caller, _ := ctx.Value(forwardedCallerKey{}).(forwardedCaller)

	return caller.anonymous
}
