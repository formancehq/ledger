package v2

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/go-libs/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsCount(t *testing.T) {
	t.Parallel()

	before := time.Now()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
	}
	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}),
			expectBackendCall: true,
		},
		{
			name: "using metadata",
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")),
			expectBackendCall: true,
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Gte("date", now.Format(time.DateFormat))),
			expectBackendCall: true,
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Gte("date", now.Format(time.DateFormat))),
			expectBackendCall: true,
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("account", "xxx")),
			expectBackendCall: true,
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("reference", "xxx")),
			expectBackendCall: true,
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("destination", "xxx")),
			expectBackendCall: true,
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}).
				WithQueryBuilder(query.Match("source", "xxx")),
			expectBackendCall: true,
		},
		{
			name: "error from backend",
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
				},
			}),
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
			expectBackendCall: true,
			returnErr:         errors.New("undefined error"),
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
			}),
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
			}),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectStatusCode == 0 {
				tc.expectStatusCode = http.StatusNoContent
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if tc.expectBackendCall {
				ledgerController.EXPECT().
					CountTransactions(gomock.Any(), ledgercontroller.NewListTransactionsQuery(tc.expectQuery)).
					Return(10, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), testing.Verbose())

			req := httptest.NewRequest(http.MethodHead, "/xxx/transactions?pit="+before.Format(time.RFC3339Nano), bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()
			if tc.queryParams != nil {
				req.URL.RawQuery = tc.queryParams.Encode()
			}

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectStatusCode < 300 && tc.expectStatusCode >= 200 {
				require.Equal(t, "10", rec.Header().Get("Count"))
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
