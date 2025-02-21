package v2

import (
	"bytes"
	"fmt"
	"github.com/formancehq/ledger/internal/api/common"
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

func TestTransactionsCount(t *testing.T) {
	t.Parallel()

	before := time.Now()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgercontroller.ResourceQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
	}
	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:    &before,
				Expand: make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "using metadata",
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("metadata[roles]", "admin"),
				Expand:  make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Gte("date", now.Format(time.DateFormat)),
				Expand:  make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$gte": {"date": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Gte("date", now.Format(time.DateFormat)),
				Expand:  make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("account", "xxx"),
				Expand:  make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("reference", "xxx"),
				Expand:  make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("destination", "xxx"),
				Expand:  make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("source", "xxx"),
				Expand:  make([]string, 0),
			},
			expectBackendCall: true,
		},
		{
			name: "error from backend",
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:    &before,
				Expand: make([]string, 0),
			},
			expectStatusCode:  http.StatusInternalServerError,
			expectedErrorCode: api.ErrorInternal,
			expectBackendCall: true,
			returnErr:         errors.New("undefined error"),
		},
		{
			name:              "with invalid query from core point of view",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrInvalidQuery{},
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:    &before,
				Expand: make([]string, 0),
			},
		},
		{
			name:              "with missing feature",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
			expectBackendCall: true,
			returnErr:         ledgercontroller.ErrMissingFeature{},
			expectQuery: ledgercontroller.ResourceQuery[any]{
				PIT:    &before,
				Expand: make([]string, 0),
			},
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
					CountTransactions(gomock.Any(), tc.expectQuery).
					Return(10, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

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
