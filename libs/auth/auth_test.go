package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthUnauthorized(t *testing.T) {

	m := Middleware()
	h := m(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Result().StatusCode)
}
