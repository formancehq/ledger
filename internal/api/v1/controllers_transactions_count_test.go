package v1

import (
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCountTransactions(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       storagecommon.ResourceQuery[any]
		expectStatusCode  int
		expectedErrorCode string
	}
	now := time.Now()

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: storagecommon.ResourceQuery[any]{},
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Match("metadata[roles]", "admin"),
			},
		},
		{
			name: "using startTime",
			queryParams: url.Values{
				"start_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Gte("date", now.Format(time.DateFormat)),
			},
		},
		{
			name: "using endTime",
			queryParams: url.Values{
				"end_time": []string{now.Format(time.DateFormat)},
			},
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Lt("date", now.Format(time.DateFormat)),
			},
		},
		{
			name: "using account",
			queryParams: url.Values{
				"account": []string{"xxx"},
			},
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Match("account", "xxx"),
			},
		},
		{
			name: "using reference",
			queryParams: url.Values{
				"reference": []string{"xxx"},
			},
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Match("reference", "xxx"),
			},
		},
		{
			name: "using destination",
			queryParams: url.Values{
				"destination": []string{"xxx"},
			},
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Match("destination", "xxx"),
			},
		},
		{
			name: "using source",
			queryParams: url.Values{
				"source": []string{"xxx"},
			},
			expectQuery: storagecommon.ResourceQuery[any]{
				Builder: query.Match("source", "xxx"),
			},
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				ledgerController.EXPECT().
					CountTransactions(gomock.Any(), testCase.expectQuery).
					Return(10, nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

			req := httptest.NewRequest(http.MethodHead, "/xxx/transactions", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

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
