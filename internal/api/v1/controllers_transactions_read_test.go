package v1

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsRead(t *testing.T) {
	t.Parallel()

	tx := ledger.ExpandedTransaction{
		Transaction: ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
		),
		PostCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": {
				"USD": ledger.NewEmptyVolumes().WithOutput(big.NewInt(100)),
			},
			"bank": {
				"USD": ledger.NewEmptyVolumes().WithInput(big.NewInt(100)),
			},
		},
	}

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		GetTransaction(gomock.Any(), ledgercontroller.NewGetTransactionQuery(0)).
		Return(&tx, nil)

	router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

	req := httptest.NewRequest(http.MethodGet, "/xxx/transactions/0", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := api.DecodeSingleResponse[ledger.ExpandedTransaction](t, rec.Body)
	require.Equal(t, tx, response)
}
