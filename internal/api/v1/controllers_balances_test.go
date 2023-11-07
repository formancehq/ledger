package v1_test

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/query"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		queryParams url.Values
		expectQuery ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilter]
	}

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{}),
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{}).
				WithQueryBuilder(query.Match("address", "foo")),
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
				GetAggregatedBalances(gomock.Any(), ledgerstore.NewGetAggregatedBalancesQuery(testCase.expectQuery)).
				Return(expectedBalances, nil)

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

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
