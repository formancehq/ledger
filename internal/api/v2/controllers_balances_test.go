package v2_test

import (
	"bytes"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/time"

	ledger "github.com/formancehq/ledger/internal"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		queryParams url.Values
		body        string
		expectQuery ledgerstore.GetAggregatedBalanceQuery
	}

	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgerstore.GetAggregatedBalanceQuery{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
			},
		},
		{
			name: "using address",
			body: `{"$match": {"address": "foo"}}`,
			expectQuery: ledgerstore.GetAggregatedBalanceQuery{
				PITFilter: ledgerstore.PITFilter{
					PIT: &now,
				},
				QueryBuilder: query.Match("address", "foo"),
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

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

			req := httptest.NewRequest(http.MethodGet, "/xxx/aggregate/balances?pit="+now.Format(time.RFC3339Nano), bytes.NewBufferString(testCase.body))
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
