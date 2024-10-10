package v2

import (
	"bytes"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/api"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetVolumes(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgercontroller.PaginatedQueryOptions[ledgercontroller.FiltersForVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}
	before := time.Now()
	zero := time.Time{}

	testCases := []testCase{
		{
			name: "basic",
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
					OOT: &zero,
				},

				UseInsertionDate: false,
			}).
				WithPageSize(DefaultPageSize),
		},
		{
			name: "using metadata",
			body: `{"$match": { "metadata[roles]": "admin" }}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")).
				WithPageSize(DefaultPageSize),
		},
		{
			name: "using account",
			body: `{"$match": { "account": "foo" }}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).
				WithQueryBuilder(query.Match("account", "foo")).
				WithPageSize(DefaultPageSize),
		},
		{
			name:              "using invalid query payload",
			body:              `[]`,
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
		},
		{
			name: "using pit",
			queryParams: url.Values{
				"pit":     []string{before.Format(time.RFC3339Nano)},
				"groupBy": []string{"3"},
			},
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
				GroupLvl: 3,
			}).WithPageSize(DefaultPageSize),
		},
		{
			name: "using Exists metadata filter",
			body: `{"$exists": { "metadata": "foo" }}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).WithPageSize(DefaultPageSize).WithQueryBuilder(query.Exists("metadata", "foo")),
		},
		{
			name: "using balance filter",
			body: `{"$gte": { "balance[EUR]": 50 }}`,
			expectQuery: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.FiltersForVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).WithQueryBuilder(query.Gte("balance[EUR]", float64(50))).
				WithPageSize(DefaultPageSize),
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
					GetVolumesWithBalances(gomock.Any(), ledgercontroller.NewGetVolumesWithBalancesQuery(testCase.expectQuery)).
					Return(&expectedCursor, nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

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
