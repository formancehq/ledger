package v2

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsCount(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
		returnErr         error
		expectBackendCall bool
	}
	before := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithPageSize(DefaultPageSize),
			expectBackendCall: true,
		},
		{
			name:              "using metadata",
			body:              `{"$match": { "metadata[roles]": "admin" }}`,
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")).
				WithPageSize(DefaultPageSize),
		},
		{
			name:              "using address",
			body:              `{"$match": { "address": "foo" }}`,
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("address", "foo")).
				WithPageSize(DefaultPageSize),
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
		{
			name:              "page size over maximum",
			expectBackendCall: true,
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithPageSize(MaxPageSize),
		},
		{
			name:              "using balance filter",
			body:              `{"$lt": { "balance[USD/2]": 100 }}`,
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Lt("balance[USD/2]", float64(100))).
				WithPageSize(DefaultPageSize),
		},
		{
			name:              "using exists filter",
			body:              `{"$exists": { "metadata": "foo" }}`,
			expectBackendCall: true,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Exists("metadata", "foo")).
				WithPageSize(DefaultPageSize),
		},
		{
			name:              "using invalid query payload",
			body:              `[]`,
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
		{
			name:              "with invalid query from core point of view",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrInvalidQuery{},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithPageSize(DefaultPageSize),
		},
		{
			name:              "with missing feature",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrMissingFeature{},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithPageSize(DefaultPageSize),
		},
		{
			name:              "with unexpected error",
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
			expectBackendCall: true,
			returnErr:         errors.New("undefined error"),
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithPageSize(DefaultPageSize),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectBackendCall {
				ledgerController.EXPECT().
					CountAccounts(gomock.Any(), ledgercontroller.NewListAccountsQuery(testCase.expectQuery)).
					Return(10, testCase.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), testing.Verbose())

			req := httptest.NewRequest(http.MethodHead, "/xxx/accounts?pit="+before.Format(time.RFC3339Nano), bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			params := url.Values{}
			if testCase.queryParams != nil {
				params = testCase.queryParams
			}
			params.Set("pit", before.Format(time.RFC3339Nano))
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				require.Equal(t, "10", rec.Header().Get("Count"))
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
