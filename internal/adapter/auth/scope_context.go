package auth

import "context"

type expandedScopesKey struct{}

// WithExpandedScopes returns a new context with the expanded granular scopes set attached.
func WithExpandedScopes(ctx context.Context, scopes map[Scope]struct{}) context.Context {
	return context.WithValue(ctx, expandedScopesKey{}, scopes)
}

// ExpandedScopesFromContext extracts the expanded granular scopes from the context.
// Returns nil if no expanded scopes are present (e.g., auth disabled).
func ExpandedScopesFromContext(ctx context.Context) map[Scope]struct{} {
	scopes, _ := ctx.Value(expandedScopesKey{}).(map[Scope]struct{})

	return scopes
}
