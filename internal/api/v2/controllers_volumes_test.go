package v2

import (
	"bytes"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/v3/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetVolumes(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       storagecommon.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]
		expectStatusCode  int
		expectedErrorCode string
	}
	before := time.Now()

	testCases := []testCase{
		{
			name: "basic",
			expectQuery: storagecommon.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgercontroller.GetVolumesOptions]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
			},
		},
		{
			name: "using metadata",
			body: `{"$match": { "metadata[roles]": "admin" }}`,
			expectQuery: storagecommon.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgercontroller.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Match("metadata[roles]", "admin"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using account",
			body: `{"$match": { "account": "foo" }}`,
			expectQuery: storagecommon.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgercontroller.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Match("account", "foo"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name:              "using invalid query payload",
			body:              `[]`,
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: common.ErrValidation,
		},
		{
			name: "using pit",
			queryParams: url.Values{
				"pit":     []string{before.Format(time.RFC3339Nano)},
				"groupBy": []string{"3"},
			},
			expectQuery: storagecommon.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgercontroller.GetVolumesOptions]{
					PIT:    &before,
					Expand: make([]string, 0),
					Opts: ledgercontroller.GetVolumesOptions{
						GroupLvl: 3,
					},
				},
			},
		},
		{
			name: "using Exists metadata filter",
			body: `{"$exists": { "metadata": "foo" }}`,
			expectQuery: storagecommon.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgercontroller.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Exists("metadata", "foo"),
					Expand:  make([]string, 0),
				},
			},
		},
		{
			name: "using balance filter",
			body: `{"$gte": { "balance[EUR]": 50 }}`,
			expectQuery: storagecommon.OffsetPaginatedQuery[ledgercontroller.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgercontroller.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Gte("balance[EUR]", float64(50)),
					Expand:  make([]string, 0),
				},
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount]{
				Data: []ledger.VolumesWithBalanceByAssetByAccount{
					{
						Account: "user:1",
						Asset:   "eur",
						VolumesWithBalance: ledger.VolumesWithBalance{
							Input:   big.NewInt(1),
							Output:  big.NewInt(1),
							Balance: big.NewInt(0),
						},
					},
				},
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				ledgerController.EXPECT().
					GetVolumesWithBalances(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop")

			req := httptest.NewRequest(http.MethodGet, "/xxx/volumes?endTime="+before.Format(time.RFC3339Nano), bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			params := url.Values{}
			if testCase.queryParams != nil {
				params = testCase.queryParams
			}

			params.Set("endTime", before.Format(time.RFC3339Nano))
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := api.DecodeCursorResponse[ledger.VolumesWithBalanceByAssetByAccount](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
