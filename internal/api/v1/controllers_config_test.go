package v1_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetInfo(t *testing.T) {
	t.Parallel()

	backend, _ := newTestingBackend(t, false)
	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	backend.
		EXPECT().
		ListLedgers(gomock.Any()).
		Return([]string{"a", "b"}, nil)

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
		Config: &v2.LedgerConfig{
			LedgerStorage: &v2.LedgerStorage{
				Driver:  "postgres",
				Ledgers: []string{"a", "b"},
			},
		},
	}, info)
}
