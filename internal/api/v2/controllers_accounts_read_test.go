package v2

import (
	"bytes"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAccountsRead(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       storagecommon.ResourceQuery[any]
		expectStatusCode  int
		expectedErrorCode string
		expectBackendCall bool
		returnErr         error
		account           string
	}
	before := time.Now()

	testCases := []testCase{
		{
			name:    "nominal",
			account: "foo",
			expectQuery: storagecommon.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("address", "foo"),
			},
			expectBackendCall: true,
		},
		{
			name:    "with expand volumes",
			account: "foo",
			expectQuery: storagecommon.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("address", "foo"),
				Expand:  []string{"volumes"},
			},
			expectBackendCall: true,
			queryParams: url.Values{
				"expand": {"volumes"},
			},
		},
		{
			name:    "with expand effective volumes",
			account: "foo",
			expectQuery: storagecommon.ResourceQuery[any]{
				PIT:     &before,
				Builder: query.Match("address", "foo"),
				Expand:  []string{"effectiveVolumes"},
			},
			expectBackendCall: true,
			queryParams: url.Values{
				"expand": {"effectiveVolumes"},
			},
		},
		{
			name:              "invalid account address",
			account:           "%8X%2F",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
	}
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {

			if tc.expectStatusCode == 0 {
				tc.expectStatusCode = http.StatusOK
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if tc.expectBackendCall {
				ledgerController.EXPECT().
					GetAccount(gomock.Any(), tc.expectQuery).
					Return(&ledger.Account{}, tc.returnErr)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodGet, "/", bytes.NewBufferString(tc.body))
			req.URL.Path = "/xxx/accounts/" + tc.account
			rec := httptest.NewRecorder()
			params := url.Values{}
			if tc.queryParams != nil {
				params = tc.queryParams
			}
			params.Set("pit", before.Format(time.RFC3339Nano))
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, tc.expectStatusCode, rec.Code)
			if tc.expectStatusCode < 300 && tc.expectStatusCode >= 200 {
				_, ok := api.DecodeSingleResponse[ledger.Account](t, rec.Body)
				require.True(t, ok)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, tc.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
