package auth

import (
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

	for _, path := range []string{"/health", "/debug/pprof/", "/debug/vars", "/v2/health", "/v2/debug/pprof/"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code, "path %s should be public", path)
	}
}

func TestHTTPAuthMiddleware_MissingToken(t *testing.T) {
	t.Parallel()

	_, keySet := testKeyPair(t)
	handler := HTTPAuthMiddleware(AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuer:  testIssuer,
	})(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/test-ledger", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHTTPAuthMiddleware_ValidToken_ClaimsInContext(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	var capturedSubject string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := ClaimsFromContext(r.Context())
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

	handler := RequireScope(AuthConfig{Enabled: false}, ScopeWrite)(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireScope_ScopesNotChecked(t *testing.T) {
	t.Parallel()

	handler := RequireScope(AuthConfig{Enabled: true, CheckScopes: false}, ScopeWrite)(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRequireScope_NoClaims(t *testing.T) {
	t.Parallel()

	handler := RequireScope(AuthConfig{Enabled: true, CheckScopes: true, Service: "ledger"}, ScopeWrite)(ok200)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/test", nil)
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRequireScope_MatchingScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{Enabled: true, KeySet: keySet, Issuer: testIssuer, Service: "ledger", CheckScopes: true}

	// Chain: auth middleware → require write → handler
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeWrite)(ok200))

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
	cfg := AuthConfig{Enabled: true, KeySet: keySet, Issuer: testIssuer, Service: "ledger", CheckScopes: true}

	// Token has read, route requires write
	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeWrite)(ok200))

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
	cfg := AuthConfig{Enabled: true, KeySet: keySet, Issuer: testIssuer, Service: "ledger", CheckScopes: true}

	handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeWrite)(ok200))

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
