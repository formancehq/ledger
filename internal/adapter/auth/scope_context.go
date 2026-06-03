package auth

import "context"

type (
	expandedScopesKey struct{}
	authPresentedKey  struct{}
)

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

// WithAuthPresented marks whether a (valid) bearer token was presented on the
// request. Used downstream to distinguish 401 (no credentials) from 403
// (credentials present but lacking the required scope).
func WithAuthPresented(ctx context.Context, presented bool) context.Context {
	return context.WithValue(ctx, authPresentedKey{}, presented)
}

// AuthPresentedFromContext reports whether the context was tagged with a
// valid bearer token. Returns false if the key is absent.
func AuthPresentedFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(authPresentedKey{}).(bool)

	return v
}
