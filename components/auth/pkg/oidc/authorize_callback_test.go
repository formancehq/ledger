package oidc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorizeError(t *testing.T) {
	handler := authorizeErrorHandler()

	req := httptest.NewRequest(http.MethodGet, "/?error=foo&error_description=bar", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	data, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	require.Equal(t, string(data), "foo : bar\n")
}
