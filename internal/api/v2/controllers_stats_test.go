package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestStats(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	expectedStats := ledgercontroller.Stats{
		Transactions: 10,
		Accounts:     5,
	}

	ledgerController.EXPECT().
		GetStats(gomock.Any()).
		Return(expectedStats, nil)

	req := httptest.NewRequest(http.MethodGet, "/xxx/stats", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	stats, ok := api.DecodeSingleResponse[ledgercontroller.Stats](t, rec.Body)
	require.True(t, ok)

	require.EqualValues(t, expectedStats, stats)
}
