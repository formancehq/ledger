package v1

import (
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/ledger/internal/storage/resources"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsRead(t *testing.T) {
	t.Parallel()

	tx := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	)

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		GetTransaction(gomock.Any(), resources.ResourceQuery[any]{
			Builder: query.Match("id", int64(0)),
		}).
		Return(&tx, nil)

	router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

	req := httptest.NewRequest(http.MethodGet, "/xxx/transactions/0", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := api.DecodeSingleResponse[ledger.Transaction](t, rec.Body)
	require.Equal(t, tx, response)
}
