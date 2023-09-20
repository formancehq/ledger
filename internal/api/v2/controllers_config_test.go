package v2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetInfo(t *testing.T) {
	t.Parallel()

	backend, _ := newTestingBackend(t)
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
