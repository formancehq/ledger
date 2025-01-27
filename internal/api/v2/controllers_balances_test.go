package v2

import (
	"bytes"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/time"

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
		body        string
		expectQuery ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]
	}

	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
				Opts:   ledgercontroller.GetAggregatedVolumesOptions{},
				PIT:    &now,
				Expand: make([]string, 0),
			},
		},
		{
			name: "using address",
			body: `{"$match": {"address": "foo"}}`,
			expectQuery: ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
				Opts:    ledgercontroller.GetAggregatedVolumesOptions{},
				PIT:     &now,
				Builder: query.Match("address", "foo"),
				Expand:  make([]string, 0),
			},
		},
		{
			name: "using exists metadata filter",
			body: `{"$exists": {"metadata": "foo"}}`,
			expectQuery: ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
				Opts:    ledgercontroller.GetAggregatedVolumesOptions{},
				PIT:     &now,
				Builder: query.Exists("metadata", "foo"),
				Expand:  make([]string, 0),
			},
		},
		{
			name: "using pit",
			queryParams: url.Values{
				"pit": []string{now.Format(time.RFC3339Nano)},
			},
			expectQuery: ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
				Opts:   ledgercontroller.GetAggregatedVolumesOptions{},
				PIT:    &now,
				Expand: make([]string, 0),
			},
		},
		{
			name: "using pit + insertion date",
			queryParams: url.Values{
				"pit":              []string{now.Format(time.RFC3339Nano)},
				"useInsertionDate": []string{"true"},
			},
			expectQuery: ledgercontroller.ResourceQuery[ledgercontroller.GetAggregatedVolumesOptions]{
				Opts: ledgercontroller.GetAggregatedVolumesOptions{
					UseInsertionDate: true,
				},
				PIT:    &now,
				Expand: make([]string, 0),
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

			req := httptest.NewRequest(http.MethodGet, "/xxx/aggregate/balances?pit="+now.Format(time.RFC3339Nano), bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			if testCase.queryParams != nil {
				req.URL.RawQuery = testCase.queryParams.Encode()
			}

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			balances, ok := api.DecodeSingleResponse[ledger.BalancesByAssets](t, rec.Body)
			require.True(t, ok)
			require.Equal(t, expectedBalances, balances)
		})
	}
}
