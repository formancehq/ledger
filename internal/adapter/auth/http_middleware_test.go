package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ok200 is a simple handler that returns 200.
var ok200 = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func TestHTTPAuthMiddleware_Disabled(t *testing.T) {
	t.Parallel()

	handler := HTTPAuthMiddleware(AuthConfig{Enabled: false})(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHTTPAuthMiddleware_PublicEndpoints(t *testing.T) {
	t.Parallel()

	_, keySet := testKeyPair(t)
	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuer:  testIssuer,
		Service: "ledger",
	})(ok200)

	for _, path := range []string{"/health", "/livez", "/readyz", "/_info"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code, "path %s should be public", path)
	}
}

func TestHTTPAuthMiddleware_MissingToken(t *testing.T) {
	t.Parallel()

	// A missing bearer token is no longer a middleware-level error; the request
	// continues with the anonymous scopes (nil here, since no mapping). The
	// 401 is then issued by RequireScope downstream if the anonymous scopes
	// don't cover the required scope.

	_, keySet := testKeyPair(t)

	var (
		capturedAuthPresented bool
		capturedScopes        map[Scope]struct{}
	)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuthPresented = AuthPresentedFromContext(r.Context())
		capturedScopes = ExpandedScopesFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuer:  testIssuer,
	})(inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.False(t, capturedAuthPresented)
	assert.Empty(t, capturedScopes)
}

func TestHTTPAuthMiddleware_MissingToken_AnonymousReadScopes(t *testing.T) {
	t.Parallel()

	// With anonymous scopes configured, an unauthenticated request inherits
	// them so downstream RequireScope can let public reads through.

	_, keySet := testKeyPair(t)

	mapping := DefaultMapping("ledger")
	mapping[ScopeMappingAnonymousKey] = []Scope{ScopeLedgersRead, ScopeAccountsRead}

	var capturedScopes map[Scope]struct{}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedScopes = ExpandedScopesFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		ScopeMapping: mapping,
	})(inner)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, HasScope(capturedScopes, ScopeLedgersRead))
	assert.True(t, HasScope(capturedScopes, ScopeAccountsRead))
	assert.False(t, HasScope(capturedScopes, ScopeTransactionsWrite))
}

func TestHTTPAuthMiddleware_ValidToken_ClaimsInContext(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)

	var capturedSubject string

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		if claims != nil {
			capturedSubject = claims.GetSubject()
		}

		w.WriteHeader(http.StatusOK)
	})

	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuer:  testIssuer,
	})(inner)

	token := signToken(t, privKey, newTestClaims("ledger:read"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test-user", capturedSubject)
}

func TestHTTPAuthMiddleware_ExpandsScopesInContext(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)

	var capturedScopes map[Scope]struct{}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedScopes = ExpandedScopesFromContext(r.Context())

		w.WriteHeader(http.StatusOK)
	})

	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}
	handler := HTTPAuthMiddleware(cfg)(inner)

	token := signToken(t, privKey, newTestClaims("ledger:read"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedScopes)
	assert.True(t, HasScope(capturedScopes, ScopeLedgersRead))
	assert.True(t, HasScope(capturedScopes, ScopeTransactionsRead))
	assert.True(t, HasScope(capturedScopes, ScopeAccountsRead))
	assert.False(t, HasScope(capturedScopes, ScopeTransactionsWrite))
}

func TestHTTPAuthMiddleware_ExpiredToken(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	claims := newTestClaims("ledger:read")
	claims.Expiration = claims.IssuedAt // expired immediately

	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuer:  testIssuer,
	})(ok200)

	token := signToken(t, privKey, claims)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHTTPAuthMiddleware_WrongIssuer(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	claims := newTestClaims("ledger:read")
	claims.Issuer = "https://wrong-issuer.example.com"

	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuer:  testIssuer,
	})(ok200)

	token := signToken(t, privKey, claims)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHTTPAuthMiddleware_InvalidSignature(t *testing.T) {
	t.Parallel()

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	_, keySet := testKeyPair(t)
	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuer:  testIssuer,
	})(ok200)

	token := signToken(t, otherKey, newTestClaims("ledger:read"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// --- RequireScope tests ---

func TestRequireScope_Disabled(t *testing.T) {
	t.Parallel()

	handler := RequireScope(AuthConfig{Enabled: false}, ScopeTransactionsWrite)(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireScope_AuthDisabled(t *testing.T) {
	t.Parallel()

	// When auth is disabled, scope check is skipped
	handler := RequireScope(AuthConfig{Enabled: false}, ScopeTransactionsWrite)(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireScope_NoClaims(t *testing.T) {
	t.Parallel()

	cfg := AuthConfig{
		Enabled:      true,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}
	handler := RequireScope(cfg, ScopeTransactionsWrite)(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRequireScope_MatchingScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}

	// Chain: auth middleware → require write → handler
	// Token has "ledger:write" which expands to include ScopeTransactionsWrite
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeTransactionsWrite)(ok200))

	token := signToken(t, privKey, newTestClaims("ledger:write"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireScope_WrongScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}

	// Token has "ledger:read" (expands to read scopes), route requires ScopeTransactionsWrite
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeTransactionsWrite)(ok200))

	token := signToken(t, privKey, newTestClaims("ledger:read"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestRequireScope_WriteScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}

	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeTransactionsWrite)(ok200))

	// With write scope → 200
	token := signToken(t, privKey, newTestClaims("ledger:write"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPut, "/test", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	// With read scope → 403
	token = signToken(t, privKey, newTestClaims("ledger:read"))
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPut, "/test", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

// TestRequireScope_LedgersRead_GatesLogListing pins the HTTP side of EN-1508:
// GET /{ledgerName}/logs sits behind requireLedgersRead, i.e.
// RequireScope(cfg, ScopeLedgersRead). It asserts the gate with GRANULAR scopes
// (identity pass-through) rather than the aggregate "ledger:read" — the
// aggregate expands to both ScopeLedgersRead and ScopeOpsRead and would mask a
// scope drift between transports.
func TestRequireScope_LedgersRead_GatesLogListing(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}

	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeLedgersRead)(ok200))

	t.Run("granular ledger-read token passes the gate", func(t *testing.T) {
		t.Parallel()

		token := signToken(t, privKey, newTestClaims(string(ScopeLedgersRead)))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/main/logs", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("granular ops-read-only token is rejected", func(t *testing.T) {
		t.Parallel()

		// Regression guard: an OpsRead-only token (the scope the old code
		// required) must NOT list a ledger's logs over HTTP.
		token := signToken(t, privKey, newTestClaims(string(ScopeOpsRead)))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/main/logs", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})
}

// --- EdDSA (Ed25519) HTTP middleware tests ---

func TestHTTPAuthMiddleware_EdDSA_ValidToken(t *testing.T) {
	t.Parallel()

	edPriv, edKeySet := ed25519TestKeyPair(t, "ed-http-key")

	var capturedSubject string

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		if claims != nil {
			capturedSubject = claims.GetSubject()
		}

		w.WriteHeader(http.StatusOK)
	})

	cfg := AuthConfig{
		Enabled: true,
		KeySet:  edKeySet,
		Service: "ledger",
		Ed25519AllowedScopes: map[string][]string{
			"ed-http-key": {"ledger:read", "ledger:write"},
		},
	}
	handler := HTTPAuthMiddleware(cfg)(inner)

	claims := newTestClaims("ledger:read")
	claims.Issuer = ""
	token := signEdDSA(t, edPriv, "ed-http-key", claims)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test-user", capturedSubject)
}

func TestHTTPAuthMiddleware_EdDSA_ExcessiveScopes(t *testing.T) {
	t.Parallel()

	edPriv, edKeySet := ed25519TestKeyPair(t, "ed-http-key")
	cfg := AuthConfig{
		Enabled: true,
		KeySet:  edKeySet,
		Service: "ledger",
		Ed25519AllowedScopes: map[string][]string{
			"ed-http-key": {"ledger:read"},
		},
	}
	handler := HTTPAuthMiddleware(cfg)(ok200)

	// Token claims admin but key only allows read
	claims := newTestClaims("ledger:admin")
	claims.Issuer = ""
	token := signEdDSA(t, edPriv, "ed-http-key", claims)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHTTPAuthMiddleware_EdDSA_UnknownKey(t *testing.T) {
	t.Parallel()

	// Sign with a key that's not in the keyset
	_, unknownPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, edKeySet := ed25519TestKeyPair(t, "known-key")

	cfg := AuthConfig{
		Enabled: true,
		KeySet:  edKeySet,
		Service: "ledger",
	}
	handler := HTTPAuthMiddleware(cfg)(ok200)

	claims := newTestClaims("ledger:read")
	claims.Issuer = ""
	token := signEdDSA(t, unknownPriv, "unknown-key", claims)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRequireScope_EdDSA_MatchingScope(t *testing.T) {
	t.Parallel()

	edPriv, edKeySet := ed25519TestKeyPair(t, "ed-http-key")
	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       edKeySet,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
		Ed25519AllowedScopes: map[string][]string{
			"ed-http-key": {"ledger:read", "ledger:write"},
		},
	}

	// Token has "ledger:write" which expands to include ScopeTransactionsWrite
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeTransactionsWrite)(ok200))

	claims := newTestClaims("ledger:write")
	claims.Issuer = ""
	token := signEdDSA(t, edPriv, "ed-http-key", claims)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHTTPAuthMiddleware_GodMode_GrantsAllScopes(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)

	var capturedScopes map[Scope]struct{}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedScopes = ExpandedScopesFromContext(r.Context())

		w.WriteHeader(http.StatusOK)
	})

	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}
	handler := HTTPAuthMiddleware(cfg)(inner)

	claims := newTestClaims()
	claims.Claims = map[string]any{"god": true}
	token := signToken(t, privKey, claims)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedScopes)

	for scope := range AllGranularScopes {
		assert.True(t, HasScope(capturedScopes, scope), "missing scope %s", scope)
	}
}

func TestRequireScope_GodMode_PassesAnyScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
	}

	// Token has no scopes but claims god mode — should pass any scope check.
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeClusterWrite)(ok200))

	claims := newTestClaims()
	claims.Claims = map[string]any{"god": true}
	token := signToken(t, privKey, claims)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

// --- Writes-only mode (anonymous = "*:read") tests ---

func writesOnlyConfig(t *testing.T) (AuthConfig, *rsa.PrivateKey) {
	t.Helper()

	privKey, keySet := testKeyPair(t)
	mapping := DefaultMapping("ledger")

	readScopes, ok := ExpandWildcardScope(WildcardRead)
	require.True(t, ok)

	mapping[ScopeMappingAnonymousKey] = readScopes

	return AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testIssuer,
		Service:      "ledger",
		ScopeMapping: mapping,
	}, privKey
}

func TestWritesOnly_Read_NoToken_Passes(t *testing.T) {
	t.Parallel()

	cfg, _ := writesOnlyConfig(t)
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeAccountsRead)(ok200))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test/accounts", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestWritesOnly_Read_InvalidToken_401(t *testing.T) {
	t.Parallel()

	cfg, _ := writesOnlyConfig(t)
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeAccountsRead)(ok200))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test/accounts", nil)
	r.Header.Set("Authorization", "Bearer garbage")
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestWritesOnly_Write_NoToken_401(t *testing.T) {
	t.Parallel()

	cfg, _ := writesOnlyConfig(t)
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeTransactionsWrite)(ok200))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test/transactions", nil)
	handler.ServeHTTP(w, r)
	// No token + write scope not covered by anonymous → 401 (please authenticate).
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestWritesOnly_Write_ValidToken_Passes(t *testing.T) {
	t.Parallel()

	cfg, privKey := writesOnlyConfig(t)
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeTransactionsWrite)(ok200))

	token := signToken(t, privKey, newTestClaims("ledger:write"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test/transactions", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestWritesOnly_Write_ValidTokenWrongScope_403(t *testing.T) {
	t.Parallel()

	cfg, privKey := writesOnlyConfig(t)
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeTransactionsWrite)(ok200))

	// Token has read-only scopes — anonymous would suffice for reads but does
	// not apply because the caller is authenticated; the token's scopes alone
	// are used. So writes are forbidden.
	token := signToken(t, privKey, newTestClaims("ledger:read"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test/transactions", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestRequireScope_EdDSA_WrongScope(t *testing.T) {
	t.Parallel()

	edPriv, edKeySet := ed25519TestKeyPair(t, "ed-http-key")
	cfg := AuthConfig{
		Enabled:      true,
		KeySet:       edKeySet,
		Service:      "ledger",
		ScopeMapping: DefaultMapping("ledger"),
		Ed25519AllowedScopes: map[string][]string{
			"ed-http-key": {"ledger:read", "ledger:write"},
		},
	}

	// Token has "ledger:read" (expands to read scopes), route requires ScopeClusterRead (admin)
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeClusterRead)(ok200))

	claims := newTestClaims("ledger:read")
	claims.Issuer = ""
	token := signEdDSA(t, edPriv, "ed-http-key", claims)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
}
