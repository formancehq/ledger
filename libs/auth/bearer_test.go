package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/oauth2/oauth2introspect"
	"github.com/golang-jwt/jwt"
	"github.com/stretchr/testify/assert"
)

func forgeToken(t *testing.T, audience string, scopes ...string) string {
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"aud":   audience,
		"scope": strings.Join(scopes, " "),
	}).SignedString([]byte("0000000000000000"))
	assert.NoError(t, err)
	return tok
}

func TestHttpBearerWithWildcardOnAudiences(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"active": true}`))
	}))
	defer srv.Close()

	i := oauth2introspect.NewIntrospecter(srv.URL)
	m := Middleware(NewHttpBearerMethod(NewIntrospectionValidator(i, true, NoAudienceValidation)))
	h := m(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agent := AgentFromContext(r.Context())
		assert.NotNil(t, agent)
		assert.Equal(t, []string{"scope1"}, agent.GetScopes())
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/foo", nil)
	req.Header.Set("Authorization", "Bearer "+forgeToken(t, "foo", "scope1"))

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
}

func TestHttpBearerWithValidAudience(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"active": true}`))
	}))
	defer srv.Close()

	i := oauth2introspect.NewIntrospecter(srv.URL)
	m := Middleware(NewHttpBearerMethod(NewIntrospectionValidator(i, false, AudienceIn("http://example.net"))))
	h := m(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotNil(t, AgentFromContext(r.Context()))
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/foo", nil)
	req.Header.Set("Authorization", "Bearer "+forgeToken(t, "http://example.net"))

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
}

func TestHttpBearerWithInvalidToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	m := Middleware(NewHttpBearerMethod(NewIntrospectionValidator(oauth2introspect.NewIntrospecter(srv.URL), true, NoAudienceValidation)))
	h := m(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/XXX", nil)
	req.Header.Set("Authorization", "Bearer XXX")

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Result().StatusCode)
}

func TestHttpBearerForbiddenWithWrongAudience(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"active": true}`))
	}))
	defer srv.Close()

	m := Middleware(NewHttpBearerMethod(NewIntrospectionValidator(
		oauth2introspect.NewIntrospecter(srv.URL),
		false,
		AudienceIn("http://example.net"),
	)))
	h := m(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/foo", nil)
	req.Header.Set("Authorization", "Bearer "+forgeToken(t, "http://external.net"))

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Result().StatusCode)
}
