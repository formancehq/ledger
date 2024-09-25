package v1_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	v1 "github.com/formancehq/ledger/v2/internal/api/v1"

	"github.com/formancehq/ledger/v2/internal/storage/systemstore"

	"github.com/formancehq/ledger/v2/internal/opentelemetry/metrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetInfo(t *testing.T) {
	t.Parallel()

	backend, _ := newTestingBackend(t, false)
	router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

	backend.
		EXPECT().
		ListLedgers(gomock.Any(), gomock.Any()).
		Return(&bunpaginate.Cursor[systemstore.Ledger]{
			Data: []systemstore.Ledger{
				{
					Name: "a",
				},
				{
					Name: "b",
				},
			},
		}, nil)

	backend.
		EXPECT().
		GetVersion().
		Return("latest")

	req := httptest.NewRequest(http.MethodGet, "/_info", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	info, _ := sharedapi.DecodeSingleResponse[v1.ConfigInfo](t, rec.Body)

	require.EqualValues(t, v1.ConfigInfo{
		Server:  "ledger",
		Version: "latest",
		Config: &v1.LedgerConfig{
			LedgerStorage: &v1.LedgerStorage{
				Driver:  "postgres",
				Ledgers: []string{"a", "b"},
			},
		},
	}, info)
}
