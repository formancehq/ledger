package v2

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/auth"
	"github.com/stretchr/testify/require"
)

func TestGetInfo(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, false)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

	req := httptest.NewRequest(http.MethodGet, "/_info", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	info := ConfigInfo{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&info))

	require.EqualValues(t, ConfigInfo{
		Server:  "ledger",
		Version: "develop",
	}, info)
}
