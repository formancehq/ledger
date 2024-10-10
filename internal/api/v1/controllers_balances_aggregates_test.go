package v1

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBalancesAggregates(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		queryParams url.Values
		expectQuery ledgercontroller.GetAggregatedBalanceQuery
	}

	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgercontroller.GetAggregatedBalanceQuery{
				UseInsertionDate: true,
			},
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: ledgercontroller.GetAggregatedBalanceQuery{
				QueryBuilder:     query.Match("address", "foo"),
				UseInsertionDate: true,
			},
		},
		{
			name: "using pit",
			queryParams: url.Values{
				"pit": []string{now.Format(time.RFC3339Nano)},
			},
			expectQuery: ledgercontroller.GetAggregatedBalanceQuery{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			},
		},
		{
			name: "using pit + insertion date",
			queryParams: url.Values{
				"pit":              []string{now.Format(time.RFC3339Nano)},
				"useInsertionDate": []string{"true"},
			},
			expectQuery: ledgercontroller.GetAggregatedBalanceQuery{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
				UseInsertionDate: true,
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

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

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
