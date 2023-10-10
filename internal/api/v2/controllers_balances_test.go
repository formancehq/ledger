package v2_test

import (
	"bytes"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		queryParams url.Values
		body        string
		expectQuery ledgerstore.PaginatedQueryOptions[ledgerstore.PITFilter]
	}

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilter{}),
		},
		{
			name: "using address",
			body: `{"$match": {"address": "foo"}}`,
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

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/aggregate/balances", bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			if testCase.queryParams != nil {
				req.URL.RawQuery = testCase.queryParams.Encode()
			}

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			balances, ok := sharedapi.DecodeSingleResponse[ledger.BalancesByAssets](t, rec.Body)
			require.True(t, ok)
			require.Equal(t, expectedBalances, balances)
		})
	}
}
