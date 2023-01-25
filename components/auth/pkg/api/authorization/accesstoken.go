package authorization

import (
	"net/http"
	"strings"

	"github.com/zitadel/oidc/pkg/oidc"
	"github.com/zitadel/oidc/pkg/op"
)

func verifyAccessToken(r *http.Request, o op.OpenIDProvider) error {
	if !strings.HasPrefix(r.URL.String(), "/clients") &&
		!strings.HasPrefix(r.URL.String(), "/scopes") &&
		!strings.HasPrefix(r.URL.String(), "/users") {
		return nil
	}

	authHeader := r.Header.Get("authorization")
	if authHeader == "" {
		return ErrMissingAuthHeader
	}

	if !strings.HasPrefix(authHeader, strings.ToLower(oidc.PrefixBearer)) &&
		!strings.HasPrefix(authHeader, oidc.PrefixBearer) {
		return ErrMalformedAuthHeader
	}

	token := strings.TrimPrefix(authHeader, strings.ToLower(oidc.PrefixBearer))
	token = strings.TrimPrefix(token, oidc.PrefixBearer)

	if _, err := op.VerifyAccessToken(r.Context(), token, o.AccessTokenVerifier()); err != nil {
		return ErrVerifyAuthToken
	}

	return nil
}
