package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	oidcclient "github.com/formancehq/go-libs/v5/pkg/authn/oidc/client"
)

// Exercise the same signed payload through both public authentication paths.
// Only aud varies: the issuer, signature, expiry and Ledger scopes remain valid.
func TestAudienceBoundary(t *testing.T) {
	t.Parallel()

	rsaKey, rsaKeySet := testKeyPair(t)
	edKey, edKeySet := ed25519TestKeyPair(t, "audience-key")
	for _, algorithm := range []struct {
		name string
		key  jose.SigningKey
	}{
		{"OIDC", jose.SigningKey{Algorithm: jose.RS256, Key: &jose.JSONWebKey{Key: rsaKey, KeyID: "test-key-id"}}},
		{"EdDSA", jose.SigningKey{Algorithm: jose.EdDSA, Key: &jose.JSONWebKey{Key: edKey, KeyID: "audience-key"}}},
	} {
		t.Run(algorithm.name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name     string
				audience any
				valid    bool
			}{
				{"matching string", "urn:formance:ledger:test", true},
				{"matching array", []string{"urn:formance:ledger:test"}, true},
				{"matching second audience", []string{"urn:formance:payments:test", "urn:formance:ledger:test"}, true},
				{"missing", nil, false},
				{"empty string", "", false},
				{"empty array", []string{}, false},
				{"same issuer other service", "urn:formance:payments:test", false},
				{"other deployment", "urn:formance:ledger:other", false},
				{"case mismatch", "urn:formance:ledger:TEST", false},
				{"wrong array", []string{"urn:formance:payments:test", "other"}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					// Exercise both configured trust sources in mixed mode.
					cfg := testAuthConfig(t, rsaKeySet)
					cfg.Ed25519KeySet = edKeySet
					cfg.Audience = "urn:formance:ledger:test"
					cfg.Ed25519AllowedScopes = map[string][]string{"audience-key": {"ledger:read"}}
					// An invalid presented token must not fall back to anonymous access.
					cfg.ScopeMapping[ScopeMappingAnonymousKey] = []Scope{ScopeLedgersRead}
					claims := newTestClaims("ledger:read")
					if algorithm.name == "EdDSA" {
						claims.Issuer = ""
					}
					payload, err := json.Marshal(claims)
					require.NoError(t, err)
					var wire map[string]any
					require.NoError(t, json.Unmarshal(payload, &wire))
					delete(wire, "aud")
					if tc.audience != nil {
						wire["aud"] = tc.audience
					}
					payload, err = json.Marshal(wire)
					require.NoError(t, err)
					signer, err := jose.NewSigner(algorithm.key, nil)
					require.NoError(t, err)
					jws, err := signer.Sign(payload)
					require.NoError(t, err)
					token, err := jws.CompactSerialize()
					require.NoError(t, err)

					ctx, grpcErr := Authenticate(ctxWithBearer(token), cfg, ScopeLedgersRead)
					called := false
					handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeLedgersRead)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						called = true
						w.WriteHeader(http.StatusOK)
					})))
					r := httptest.NewRequest(http.MethodGet, "/main/logs", nil)
					r.Header.Set("Authorization", "Bearer "+token)
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					if tc.valid || algorithm.name == "EdDSA" {
						require.NoError(t, grpcErr)
						require.True(t, HasScope(ExpandedScopesFromContext(ctx), ScopeLedgersRead))
						require.Equal(t, http.StatusOK, w.Code)
						require.True(t, called)
					} else {
						assert.Equal(t, codes.Unauthenticated, status.Code(grpcErr))
						if assert.Error(t, grpcErr) {
							assert.Contains(t, grpcErr.Error(), "audience")
						}
						assert.Nil(t, claimsFromContext(ctx))
						assert.Empty(t, ExpandedScopesFromContext(ctx))
						assert.Equal(t, http.StatusUnauthorized, w.Code)
						assert.Contains(t, w.Body.String(), "audience")
						assert.False(t, called)
					}
				})
			}
		})
	}
}

func TestOIDCEdDSARequiresAudience(t *testing.T) {
	t.Parallel()

	privateKey, _ := ed25519TestKeyPair(t, "oidc-ed-key")
	_, staticKeys := ed25519TestKeyPair(t, "oidc-ed-key")
	jwks, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key: privateKey.Public(), KeyID: "oidc-ed-key", Algorithm: string(jose.EdDSA), Use: "sig",
	}}})
	require.NoError(t, err)
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write(jwks)
		assert.NoError(t, err)
	}))
	t.Cleanup(issuer.Close)

	for _, mixed := range []bool{false, true} {
		for _, tc := range []struct {
			name            string
			audience        oidc.Audience
			wrongIssuer     bool
			missingExpected bool
			wantError       string
		}{
			{name: "missing audience", wantError: "audience"},
			{name: "other resource", audience: oidc.Audience{"other"}, wantError: "audience"},
			{name: "missing expected audience", audience: oidc.Audience{"urn:formance:ledger:test"}, missingExpected: true, wantError: "audience"},
			{name: "matching audience", audience: oidc.Audience{"urn:formance:ledger:test"}},
			{name: "wrong issuer", audience: oidc.Audience{"urn:formance:ledger:test"}, wrongIssuer: true, wantError: "issuer"},
		} {
			t.Run(fmt.Sprintf("mixed=%v/%s", mixed, tc.name), func(t *testing.T) {
				t.Parallel()
				cfg := testAuthConfig(t, oidcclient.NewRemoteKeySet(issuer.Client(), issuer.URL))
				cfg.Issuer = issuer.URL
				if mixed {
					cfg.Ed25519KeySet = staticKeys
					// The remote key has the same ID but different key material.
					cfg.Ed25519AllowedScopes = map[string][]string{"oidc-ed-key": {"ledger:read"}}
					cfg.Ed25519GodKeys = map[string]bool{"oidc-ed-key": true}
				}
				if tc.missingExpected {
					cfg.Audience = ""
				}
				claims := newTestClaims("ledger:read")
				claims.Issuer = issuer.URL
				if tc.wrongIssuer {
					claims.Issuer = "https://wrong-issuer.example.com"
				}
				claims.Audience = tc.audience
				token := signEdDSA(t, privateKey, "oidc-ed-key", claims)
				_, grpcErr := Authenticate(ctxWithBearer(token), cfg, ScopeLedgersRead)
				r := httptest.NewRequest(http.MethodGet, "/main/logs", nil)
				r.Header.Set("Authorization", "Bearer "+token)
				w := httptest.NewRecorder()
				HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeLedgersRead)(ok200)).ServeHTTP(w, r)
				if tc.wantError != "" {
					require.Equal(t, codes.Unauthenticated, status.Code(grpcErr))
					require.Contains(t, grpcErr.Error(), tc.wantError)
					require.Equal(t, http.StatusUnauthorized, w.Code)
					require.Contains(t, w.Body.String(), tc.wantError)
				} else {
					require.NoError(t, grpcErr)
					require.Equal(t, http.StatusOK, w.Code)
				}
			})
		}
	}
}

func TestAudienceGodModeContract(t *testing.T) {
	t.Parallel()

	rsaKey, rsaKeySet := testKeyPair(t)
	edKey, edKeySet := ed25519TestKeyPair(t, "god-key")
	for _, edDSA := range []bool{false, true} {
		for _, expected := range []string{"", " \t", "urn:formance:ledger:test"} {
			t.Run(fmt.Sprintf("EdDSA=%v/expected=%q", edDSA, expected), func(t *testing.T) {
				t.Parallel()
				claims := newTestClaims()
				claims.Audience = oidc.Audience{"urn:formance:payments:test"}
				if expected == "" || expected == " \t" {
					// Even a matching empty claim must not authorize a missing configuration.
					claims.Audience = oidc.Audience{expected}
				}
				claims.Claims = map[string]any{"god": true}
				cfg := testAuthConfig(t, rsaKeySet)
				token := signToken(t, rsaKey, claims)
				if edDSA {
					claims.Issuer = ""
					token = signEdDSA(t, edKey, "god-key", claims)
					cfg.KeySet = nil
					cfg.Ed25519KeySet = edKeySet
					cfg.Ed25519AllowedScopes = map[string][]string{"god-key": nil}
					cfg.Ed25519GodKeys = map[string]bool{"god-key": true}
				}
				cfg.Audience = expected
				_, err := Authenticate(ctxWithBearer(token), cfg, ScopeClusterWrite)
				if edDSA {
					require.NoError(t, err)
				} else {
					require.Equal(t, codes.Unauthenticated, status.Code(err))
					require.Contains(t, err.Error(), "audience")
				}
				handler := HTTPAuthMiddleware(cfg)(ok200)
				r := httptest.NewRequest(http.MethodGet, "/main/logs", nil)
				r.Header.Set("Authorization", "Bearer "+token)
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				if edDSA {
					require.Equal(t, http.StatusOK, w.Code)
				} else {
					require.Equal(t, http.StatusUnauthorized, w.Code)
					require.Contains(t, w.Body.String(), "audience")
				}
			})
		}
	}
}
