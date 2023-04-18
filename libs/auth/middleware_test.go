package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNeedScopesMiddleware(t *testing.T) {

	checkAuthMiddleware := Middleware(NewHttpBearerMethod(NoOpValidator))
	needConsentMiddleware := NeedAllScopes("scope1")

	h := checkAuthMiddleware(needConsentMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agent := AgentFromContext(r.Context())
		assert.NotNil(t, agent)
		assert.Equal(t, []string{"scope1"}, agent.GetScopes())
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/foo", nil)
	req.Header.Set("Authorization", "Bearer "+forgeToken(t, "foo", "scope1"))

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
}

func TestNeedScopesMiddlewareFailure(t *testing.T) {

	checkAuthMiddleware := Middleware(NewHttpBearerMethod(NoOpValidator))
	needConsentMiddleware := NeedAllScopes("scope2")

	h := checkAuthMiddleware(needConsentMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/foo", nil)
	req.Header.Set("Authorization", "Bearer "+forgeToken(t, "foo", "scope1"))

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Result().StatusCode)
}

func TestNeedOneOfScopes(t *testing.T) {

	checkAuthMiddleware := Middleware(NewHttpBearerMethod(NoOpValidator))
	needConsentMiddleware := NeedOneOfScopes("A", "B", "C")

	h := checkAuthMiddleware(needConsentMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agent := AgentFromContext(r.Context())
		assert.NotNil(t, agent)
		assert.Equal(t, []string{"B", "X", "Y"}, agent.GetScopes())
	})))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/foo", nil)
	req.Header.Set("Authorization", "Bearer "+forgeToken(t, "foo", "B", "X", "Y"))

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
}
