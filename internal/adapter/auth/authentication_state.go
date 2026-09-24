package auth

import "context"

type authenticationStateKey struct{}

// AuthenticationState is the immutable result of evaluating request
// credentials. Its fields stay private so authorization code can only inspect
// it through the package's fail-closed helpers.
type AuthenticationState struct {
	enabled   bool
	presented bool
	scopes    map[Scope]struct{}
}

func withAuthenticationState(ctx context.Context, enabled, presented bool, scopes map[Scope]struct{}) context.Context {
	copyScopes := make(map[Scope]struct{}, len(scopes))
	for scope := range scopes {
		copyScopes[scope] = struct{}{}
	}

	return context.WithValue(ctx, authenticationStateKey{}, AuthenticationState{
		enabled:   enabled,
		presented: presented,
		scopes:    copyScopes,
	})
}

func authenticationStateFromContext(ctx context.Context) (AuthenticationState, bool) {
	state, ok := ctx.Value(authenticationStateKey{}).(AuthenticationState)

	return state, ok
}
