package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

func TestInfoHandler(t *testing.T) {
	t.Parallel()

	h := infoHandler(version.Info{
		Version:   "v3.1.0",
		Commit:    "abc1234",
		BuildDate: "2026-06-19T00:00:00Z",
		GoVersion: "go1.24",
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_info", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "v3.1.0", got["version"])
	require.Equal(t, "abc1234", got["commit"])
	require.Equal(t, "2026-06-19T00:00:00Z", got["buildDate"])
	require.Equal(t, "go1.24", got["goVersion"])
	_, wrapped := got["data"]
	require.False(t, wrapped)
}
