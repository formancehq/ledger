package v2_test

import (
	"bytes"

	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/time"

	sharedapi "github.com/formancehq/go-libs/api"
	ledger "github.com/formancehq/ledger/v2/internal"
	v2 "github.com/formancehq/ledger/v2/internal/api/v2"
	"github.com/formancehq/ledger/v2/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/v2/internal/storage/ledgerstore"

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
		expectQuery       ledgerstore.PaginatedQueryOptions[ledgerstore.FiltersForVolumes]
		expectStatusCode  int
		expectedErrorCode string
	}
	before := time.Now()
	zero := time.Time{}

	testCases := []testCase{
		{
			name: "basic",
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
					OOT: &zero,
				},

				UseInsertionDate: false,
			}).
				WithPageSize(v2.DefaultPageSize),
		},
		{
			name: "using metadata",
			body: `{"$match": { "metadata[roles]": "admin" }}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin")).
				WithPageSize(v2.DefaultPageSize),
		},
		{
			name: "using account",
			body: `{"$match": { "account": "foo" }}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).
				WithQueryBuilder(query.Match("account", "foo")).
				WithPageSize(v2.DefaultPageSize),
		},
		{
			name:              "using invalid query payload",
			body:              `[]`,
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: v2.ErrValidation,
		},
		{
			name: "using pit",
			queryParams: url.Values{
				"pit":     []string{before.Format(time.RFC3339Nano)},
				"groupBy": []string{"3"},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
				GroupLvl: 3,
			}).WithPageSize(v2.DefaultPageSize),
		},
		{
			name: "using Exists metadata filter",
			body: `{"$exists": { "metadata": "foo" }}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).WithPageSize(v2.DefaultPageSize).WithQueryBuilder(query.Exists("metadata", "foo")),
		},
		{
			name: "using balance filter",
			body: `{"$gte": { "balance[EUR]": 50 }}`,
			expectQuery: ledgerstore.NewPaginatedQueryOptions(ledgerstore.FiltersForVolumes{
				PITFilter: ledgerstore.PITFilter{
					PIT: &before,
					OOT: &zero,
				},
			}).WithQueryBuilder(query.Gte("balance[EUR]", float64(50))).
				WithPageSize(v2.DefaultPageSize),
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

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetVolumesWithBalances(gomock.Any(), ledgerstore.NewGetVolumesWithBalancesQuery(testCase.expectQuery)).
					Return(&expectedCursor, nil)
			}

			router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

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
				cursor := sharedapi.DecodeCursorResponse[ledger.VolumesWithBalanceByAssetByAccount](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
