package auth

import (
	"context"
	"sort"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type (
	clusterInternalKey   struct{}
	forwardedSnapshotKey struct{}
	systemActorKey       struct{}
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

// WithForwardedSnapshot attaches a caller snapshot captured by a forwarding
// follower. Only handlers that have verified the request is cluster-internal
// should call this; the value is otherwise untrusted.
func WithForwardedSnapshot(ctx context.Context, snapshot *commonpb.CallerSnapshot) context.Context {
	return context.WithValue(ctx, forwardedSnapshotKey{}, snapshot)
}

// ForwardedSnapshotFromContext returns the forwarded caller snapshot, or nil
// when none was attached (direct request, or no auth presented at the
// forwarding hop).
func ForwardedSnapshotFromContext(ctx context.Context) *commonpb.CallerSnapshot {
	c, _ := ctx.Value(forwardedSnapshotKey{}).(*commonpb.CallerSnapshot)

	return c
}

// WithSystemActor marks the context as a system/internal action attributed to
// the named component (see commands.Component*). ResolveCallerSnapshot turns
// it into a system CallerSnapshot, so system proposals routed through
// admission (chapter archiver, sealer, schedulers) are attributed to that
// component.
func WithSystemActor(ctx context.Context, component string) context.Context {
	return context.WithValue(ctx, systemActorKey{}, component)
}

// systemActorFromContext returns the system component set by WithSystemActor,
// and whether one was set.
func systemActorFromContext(ctx context.Context) (string, bool) {
	c, ok := ctx.Value(systemActorKey{}).(string)

	return c, ok && c != ""
}

// ResolveCallerSnapshot returns the caller snapshot for the current context,
// in precedence order:
//  1. an explicit system actor (WithSystemActor) — a background action;
//  2. an explicitly forwarded snapshot (set by a follower that already
//     validated the user JWT/Ed25519 token);
//  3. one built from the claims attached locally by Authenticate.
//
// Returns nil only when the request is an unauthenticated user request.
//
// Use this from both the follower (when forwarding to the leader, to keep the
// original snapshot intact across hops) and the leader (when building the
// proposal carried through Raft).
func ResolveCallerSnapshot(ctx context.Context) *commonpb.CallerSnapshot {
	if component, ok := systemActorFromContext(ctx); ok {
		return commands.SystemCallerSnapshot(component)
	}

	if forwarded := ForwardedSnapshotFromContext(ctx); forwarded != nil {
		return forwarded
	}

	return buildCallerSnapshot(ctx)
}

// buildCallerSnapshot freezes the admission-time auth state of the current
// context into a CallerSnapshot. Returns nil when no claims are present.
//
// The result conflates two concerns on purpose — caller identity (subject +
// source) and the authorization granted at admission (scopes + god) —
// because the FSM persists the whole thing into AuditEntry so the audit log
// records both who did the operation and what they were allowed to do at
// that moment. Downstream code MUST NOT re-derive permissions from this
// struct: it is a frozen admission-time snapshot.
func buildCallerSnapshot(ctx context.Context) *commonpb.CallerSnapshot {
	claims := ClaimsFromContext(ctx)
	if claims == nil {
		return nil
	}

	identity := &commonpb.CallerIdentity{
		Subject: claims.Subject,
	}

	// Source: key_id for Ed25519, issuer for OIDC.
	if keyID := KeyIDFromContext(ctx); keyID != "" {
		identity.Source = &commonpb.CallerIdentity_KeyId{KeyId: keyID}
	} else if claims.Issuer != "" {
		identity.Source = &commonpb.CallerIdentity_Issuer{Issuer: claims.Issuer}
	}

	snapshot := &commonpb.CallerSnapshot{
		Identity: identity,
	}

	if god, ok := claims.Claims["god"].(bool); ok && god {
		snapshot.God = true
	}

	// Sorted scopes for deterministic FSM serialization.
	if expanded := ExpandedScopesFromContext(ctx); len(expanded) > 0 {
		scopes := make([]string, 0, len(expanded))
		for s := range expanded {
			scopes = append(scopes, string(s))
		}

		sort.Strings(scopes)
		snapshot.Scopes = scopes
	}

	return snapshot
}
