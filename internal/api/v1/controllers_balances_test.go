package v1_test

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/time"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		queryParams url.Values
		expectQuery ledgerstore.GetAggregatedBalanceQuery
	}

	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgerstore.GetAggregatedBalanceQuery{
				UseInsertionDate: true,
			},
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: ledgerstore.GetAggregatedBalanceQuery{
				QueryBuilder:     query.Match("address", "foo"),
				UseInsertionDate: true,
			},
		},
		{
			name: "using pit",
			queryParams: url.Values{
				"pit": []string{now.Format(time.RFC3339Nano)},
			},
			expectQuery: ledgerstore.GetAggregatedBalanceQuery{
				PITFilter: ledgerstore.PITFilter{
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
			expectQuery: ledgerstore.GetAggregatedBalanceQuery{
				PITFilter: ledgerstore.PITFilter{
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
			backend, mock := newTestingBackend(t, true)
			mock.EXPECT().
				GetAggregatedBalances(gomock.Any(), testCase.expectQuery).
				Return(expectedBalances, nil)

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

			req := httptest.NewRequest(http.MethodGet, "/xxx/aggregate/balances", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			balances, ok := sharedapi.DecodeSingleResponse[ledger.BalancesByAssets](t, rec.Body)
			require.True(t, ok)
			require.Equal(t, expectedBalances, balances)
		})
	}
}
