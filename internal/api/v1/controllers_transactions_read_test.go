package v1

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/query"

	ledger "github.com/formancehq/ledger/internal"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func TestTransactionsRead(t *testing.T) {
	t.Parallel()

	tx := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
	)

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		GetTransaction(gomock.Any(), storagecommon.ResourceQuery[any]{
			Builder: query.Match("id", uint64(0)),
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
