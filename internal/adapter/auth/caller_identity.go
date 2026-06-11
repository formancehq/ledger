package auth

import (
	"context"
	"sort"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type (
	clusterInternalKey struct{}
	forwardedCallerKey struct{}
)

// WithClusterInternal marks the context as belonging to a request that
// authenticated via the cluster-secret (peer-to-peer trust boundary).
// It allows downstream handlers to distinguish forwarded requests from
// direct user requests when deciding which fields to trust.
func WithClusterInternal(ctx context.Context, internal bool) context.Context {
	return context.WithValue(ctx, clusterInternalKey{}, internal)
}

// IsClusterInternal reports whether the request authenticated via the
// cluster-secret fast path. False when the marker is absent.
func IsClusterInternal(ctx context.Context) bool {
	v, _ := ctx.Value(clusterInternalKey{}).(bool)

	return v
}

// WithForwardedCaller attaches a caller identity captured by a forwarding
// follower. Only handlers that have verified the request is cluster-internal
// should call this; the value is otherwise untrusted.
func WithForwardedCaller(ctx context.Context, caller *commonpb.CallerIdentity) context.Context {
	return context.WithValue(ctx, forwardedCallerKey{}, caller)
}

// ForwardedCallerFromContext returns the forwarded caller identity, or nil
// when none was attached (direct request, or no auth presented at the
// forwarding hop).
func ForwardedCallerFromContext(ctx context.Context) *commonpb.CallerIdentity {
	c, _ := ctx.Value(forwardedCallerKey{}).(*commonpb.CallerIdentity)

	return c
}

// ResolveCallerIdentity returns the authenticated caller for the current
// context. It honors an explicitly forwarded caller (set by a follower that
// already validated the user JWT/Ed25519 token) before falling back to the
// claims attached locally by Authenticate. Returns nil when the request is
// unauthenticated or anonymous.
//
// Use this from both the follower (when forwarding to the leader, to keep
// the original identity intact across hops) and the leader (when building
// the proposal carried through Raft).
func ResolveCallerIdentity(ctx context.Context) *commonpb.CallerIdentity {
	if forwarded := ForwardedCallerFromContext(ctx); forwarded != nil {
		return forwarded
	}

	claims := ClaimsFromContext(ctx)
	if claims == nil {
		return nil
	}

	identity := &commonpb.CallerIdentity{
		Subject: claims.Subject,
	}

	if god, ok := claims.Claims["god"].(bool); ok && god {
		identity.God = true
	}

	if expanded := ExpandedScopesFromContext(ctx); len(expanded) > 0 {
		scopes := make([]string, 0, len(expanded))
		for s := range expanded {
			scopes = append(scopes, string(s))
		}

		sort.Strings(scopes)
		identity.Scopes = scopes
	}

	if keyID := KeyIDFromContext(ctx); keyID != "" {
		identity.Source = &commonpb.CallerIdentity_KeyId{KeyId: keyID}
	} else if claims.Issuer != "" {
		identity.Source = &commonpb.CallerIdentity_Issuer{Issuer: claims.Issuer}
	}

	return identity
}
