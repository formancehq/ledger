package v1

import (
	"github.com/formancehq/ledger/internal/storage/resources"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBalancesAggregates(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		queryParams url.Values
		expectQuery resources.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]
	}

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: resources.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
				Opts: ledgercontroller.GetAggregatedVolumesOptions{
					UseInsertionDate: true,
				},
			},
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: resources.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
				Opts: ledgercontroller.GetAggregatedVolumesOptions{
					UseInsertionDate: true,
				},
				Builder: query.Match("address", "foo"),
			},
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			expectedBalances := ledger.BalancesByAssets{
				"world": big.NewInt(-100),
			}
			systemController, ledgerController := newTestingSystemController(t, true)
			ledgerController.EXPECT().
				GetAggregatedBalances(gomock.Any(), testCase.expectQuery).
				Return(expectedBalances, nil)

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodGet, "/xxx/aggregate/balances", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			balances, ok := api.DecodeSingleResponse[ledger.BalancesByAssets](t, rec.Body)
			require.True(t, ok)
			require.Equal(t, expectedBalances, balances)
		})
	}
}
