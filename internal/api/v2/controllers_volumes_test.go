package v2

import (
	"bytes"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

func TestVolumesList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       storagecommon.PaginatedQuery[ledgerstore.GetVolumesOptions]
		expectStatusCode  int
		expectedErrorCode string
	}
	before := time.Now()

	testCases := []testCase{
		{
			name: "basic",
			expectQuery: storagecommon.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgerstore.GetVolumesOptions]{
					PIT:    &before,
					Expand: make([]string, 0),
				},
				Column: "account",
				Order:  pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			},
		},
		{
			name: "using metadata",
			body: `{"$match": { "metadata[roles]": "admin" }}`,
			expectQuery: storagecommon.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgerstore.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Match("metadata[roles]", "admin"),
					Expand:  make([]string, 0),
				},
				Column: "account",
				Order:  pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			},
		},
		{
			name: "using account",
			body: `{"$match": { "account": "foo" }}`,
			expectQuery: storagecommon.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgerstore.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Match("account", "foo"),
					Expand:  make([]string, 0),
				},
				Column: "account",
				Order:  pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
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
			expectQuery: storagecommon.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgerstore.GetVolumesOptions]{
					PIT:    &before,
					Expand: make([]string, 0),
					Opts: ledgerstore.GetVolumesOptions{
						GroupLvl: 3,
					},
				},
				Column: "account",
				Order:  pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			},
		},
		{
			name: "using exists metadata filter",
			body: `{"$exists": { "metadata": "foo" }}`,
			expectQuery: storagecommon.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgerstore.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Exists("metadata", "foo"),
					Expand:  make([]string, 0),
				},
				Column: "account",
				Order:  pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			},
		},
		{
			name: "using balance filter",
			body: `{"$gte": { "balance[EUR]": 50 }}`,
			expectQuery: storagecommon.InitialPaginatedQuery[ledgerstore.GetVolumesOptions]{
				PageSize: bunpaginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[ledgerstore.GetVolumesOptions]{
					PIT:     &before,
					Builder: query.Gte("balance[EUR]", big.NewInt(50)),
					Expand:  make([]string, 0),
				},
				Column: "account",
				Order:  pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
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
