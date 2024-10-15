package v2

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsRead(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tx := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	)

	query := ledgercontroller.NewGetTransactionQuery(0)
	query.PIT = &now

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		GetTransaction(gomock.Any(), query).
		Return(&tx, nil)

	router := NewRouter(systemController, auth.NewNoAuth(), testing.Verbose())

	req := httptest.NewRequest(http.MethodGet, "/xxx/transactions/0?pit="+now.Format(time.RFC3339Nano), nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := api.DecodeSingleResponse[ledger.Transaction](t, rec.Body)
	require.Equal(t, tx, response)
}
