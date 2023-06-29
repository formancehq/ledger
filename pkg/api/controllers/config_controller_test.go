package controllers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetInfo(t *testing.T) {
	t.Parallel()

	backend, _ := newTestingBackend(t)
	router := routes.NewRouter(backend, nil, metrics.NewNoOpRegistry())

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

	info := controllers.ConfigInfo{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&info))

	require.EqualValues(t, controllers.ConfigInfo{
		Server:  "ledger",
		Version: "latest",
		Config: &controllers.Config{
			LedgerStorage: &controllers.LedgerStorage{
				Driver:  "postgres",
				Ledgers: []string{"a", "b"},
			},
		},
	}, info)
}
