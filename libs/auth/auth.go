package auth

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/zitadel/oidc/v2/pkg/client/rp"
	"github.com/zitadel/oidc/v2/pkg/oidc"
	"github.com/zitadel/oidc/v2/pkg/op"
	"go.uber.org/zap"
)

type jwtAuth struct {
	logger              logging.Logger
	httpClient          *http.Client
	accessTokenVerifier op.AccessTokenVerifier

	issuer      string
	checkScopes bool
	service     string
}

func newOtlpHttpClient(maxRetries int) *http.Client {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = maxRetries
	return retryClient.StandardClient()
}

func newJWTAuth(
	logger logging.Logger,
	readKeySetMaxRetries int,
	issuer string,
	service string,
	checkScopes bool,
) *jwtAuth {
	return &jwtAuth{
		logger:              logger,
		httpClient:          newOtlpHttpClient(readKeySetMaxRetries),
		accessTokenVerifier: nil,
		issuer:              issuer,
		checkScopes:         checkScopes,
		service:             service,
	}
}

// Authenticate validates the JWT in the request and returns the user, if valid.
func (ja *jwtAuth) Authenticate(w http.ResponseWriter, r *http.Request) (bool, error) {
	authHeader := r.Header.Get("authorization")
	if authHeader == "" {
		ja.logger.Error("no authorization header")
		return false, fmt.Errorf("no authorization header")
	}

	if !strings.HasPrefix(authHeader, strings.ToLower(oidc.PrefixBearer)) &&
		!strings.HasPrefix(authHeader, oidc.PrefixBearer) {
		ja.logger.Error("malformed authorization header")
		return false, fmt.Errorf("malformed authorization header")
	}

	token := strings.TrimPrefix(authHeader, strings.ToLower(oidc.PrefixBearer))
	token = strings.TrimPrefix(token, oidc.PrefixBearer)

	accessTokenVerifier, err := ja.getAccessTokenVerifier(r.Context())
	if err != nil {
		ja.logger.Error("unable to create access token verifier", zap.Error(err))
		return false, fmt.Errorf("unable to create access token verifier: %w", err)
	}

	claims, err := op.VerifyAccessToken[*oidc.AccessTokenClaims](r.Context(), token, accessTokenVerifier)
	if err != nil {
		ja.logger.Error("unable to verify access token", zap.Error(err))
		return false, fmt.Errorf("unable to verify access token: %w", err)
	}

	if ja.checkScopes {
		scope := claims.Scopes

		allowed := true
		switch r.Method {
		case http.MethodOptions, http.MethodGet, http.MethodHead, http.MethodTrace:
			allowed = allowed && (collectionutils.Contains(scope, ja.service+":read") || collectionutils.Contains(scope, ja.service+":write"))
		default:
			allowed = allowed && collectionutils.Contains(scope, ja.service+":write")
		}

		if !allowed {
			ja.logger.Info("not enough scopes")
			return false, fmt.Errorf("missing access, found scopes: '%s' need %s:read|write", strings.Join(scope, ", "), ja.service)
		}
	}

	return true, nil
}

func (ja *jwtAuth) getAccessTokenVerifier(ctx context.Context) (op.AccessTokenVerifier, error) {
	if ja.accessTokenVerifier == nil {
		//discoveryConfiguration, err := client.Discover(ja.Issuer, ja.httpClient)
		//if err != nil {
		//	return nil, err
		//}

		// todo: ugly quick fix
		authServicePort := "8080"
		if fromEnv := os.Getenv("AUTH_SERVICE_PORT"); fromEnv != "" {
			authServicePort = fromEnv
		}
		keySet := rp.NewRemoteKeySet(ja.httpClient, fmt.Sprintf("http://auth:%s/keys", authServicePort))

		ja.accessTokenVerifier = op.NewAccessTokenVerifier(
			os.Getenv("STACK_PUBLIC_URL")+"/api/auth",
			keySet,
		)
	}

	return ja.accessTokenVerifier, nil
}
