package v2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/stretchr/testify/require"
)

func TestGetInfo(t *testing.T) {
	t.Parallel()

	backend, _ := newTestingBackend(t, false)
	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

	backend.
		EXPECT().
		GetVersion().
		Return("latest")

	req := httptest.NewRequest(http.MethodGet, "/_info", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	info := v2.ConfigInfo{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&info))

	require.EqualValues(t, v2.ConfigInfo{
		Server:  "ledger",
		Version: "latest",
	}, info)
}
