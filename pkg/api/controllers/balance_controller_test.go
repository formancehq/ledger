package controllers_test

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		queryParams url.Values
		expectQuery ledgerstore.BalancesQuery
	}

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewBalancesQuery(),
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: ledgerstore.NewBalancesQuery().WithAddressFilter("foo"),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			expectedBalances := core.BalancesByAssets{
				"world": big.NewInt(-100),
			}
			backend, mock := newTestingBackend(t)
			mock.EXPECT().
				GetBalancesAggregated(gomock.Any(), testCase.expectQuery).
				Return(expectedBalances, nil)

			router := routes.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/aggregate/balances", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			balances, ok := sharedapi.DecodeSingleResponse[core.BalancesByAssets](t, rec.Body)
			require.True(t, ok)
			require.Equal(t, expectedBalances, balances)
		})
	}
}

func TestGetBalances(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgerstore.BalancesQuery
		expectStatusCode  int
		expectedErrorCode string
	}

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewBalancesQuery(),
		},
		{
			name: "empty cursor with other param",
			queryParams: url.Values{
				"cursor": []string{ledgerstore.EncodeCursor(ledgerstore.NewBalancesQuery())},
				"after":  []string{"bob"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using after",
			queryParams: url.Values{
				"after": []string{"foo"},
			},
			expectQuery: ledgerstore.NewBalancesQuery().WithAfterAddress("foo"),
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: ledgerstore.NewBalancesQuery().WithAddressFilter("foo"),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := sharedapi.Cursor[core.BalancesByAssetsByAccounts]{
				Data: []core.BalancesByAssetsByAccounts{
					{
						"world": core.BalancesByAssets{
							"USD": big.NewInt(100),
						},
					},
				},
			}

			backend, mock := newTestingBackend(t)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mock.EXPECT().
					GetBalances(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, nil)
			}

			router := routes.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/balances", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := sharedapi.DecodeCursorResponse[core.BalancesByAssetsByAccounts](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
