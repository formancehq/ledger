package v2

import (
	"bytes"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTransactionsList(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		body              string
		expectQuery       ledgercontroller.ListTransactionsQuery
		expectStatusCode  int
		expectedErrorCode string
	}
	now := time.Now()

	testCases := []testCase{
		{
			name: "nominal",
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			})),
		},
		{
			name: "using metadata",
			body: `{"$match": {"metadata[roles]": "admin"}}`,
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("metadata[roles]", "admin"))),
		},
		{
			name: "using startTime",
			body: fmt.Sprintf(`{"$gte": {"start_time": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Gte("start_time", now.Format(time.DateFormat)))),
		},
		{
			name: "using endTime",
			body: fmt.Sprintf(`{"$lte": {"end_time": "%s"}}`, now.Format(time.DateFormat)),
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Lte("end_time", now.Format(time.DateFormat)))),
		},
		{
			name: "using account",
			body: `{"$match": {"account": "xxx"}}`,
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("account", "xxx"))),
		},
		{
			name: "using reference",
			body: `{"$match": {"reference": "xxx"}}`,
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("reference", "xxx"))),
		},
		{
			name: "using destination",
			body: `{"$match": {"destination": "xxx"}}`,
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("destination", "xxx"))),
		},
		{
			name: "using source",
			body: `{"$match": {"source": "xxx"}}`,
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Match("source", "xxx"))),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{bunpaginate.EncodeCursor(ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{})))},
			},
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{},
			})),
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"XXX"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: ErrValidation,
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
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithPageSize(MaxPageSize)),
		},
		{
			name: "using cursor",
			queryParams: url.Values{
				"cursor": []string{"eyJwYWdlU2l6ZSI6MTUsImJvdHRvbSI6bnVsbCwiY29sdW1uIjoiaWQiLCJwYWdpbmF0aW9uSUQiOm51bGwsIm9yZGVyIjoxLCJmaWx0ZXJzIjp7InFiIjp7fSwicGFnZVNpemUiOjE1LCJvcHRpb25zIjp7InBpdCI6bnVsbCwidm9sdW1lcyI6ZmFsc2UsImVmZmVjdGl2ZVZvbHVtZXMiOmZhbHNlfX0sInJldmVyc2UiOmZhbHNlfQ"},
			},
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{})),
		},
		{
			name: "using $exists metadata filter",
			body: `{"$exists": {"metadata": "foo"}}`,
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			}).
				WithQueryBuilder(query.Exists("metadata", "foo"))),
		},
		{
			name:        "paginate using effective order",
			queryParams: map[string][]string{"order": {"effective"}},
			expectQuery: ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: &now,
				},
			})).
				WithColumn("timestamp"),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := bunpaginate.Cursor[ledger.Transaction]{
				Data: []ledger.Transaction{
					ledger.NewTransaction().WithPostings(
						ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
					),
				},
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				ledgerController.EXPECT().
					ListTransactions(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, nil)
			}

			router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

			req := httptest.NewRequest(http.MethodGet, "/xxx/transactions", bytes.NewBufferString(testCase.body))
			rec := httptest.NewRecorder()
			params := url.Values{}
			if testCase.queryParams != nil {
				params = testCase.queryParams
			}
			params.Set("pit", now.Format(time.RFC3339Nano))
			req.URL.RawQuery = params.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := api.DecodeCursorResponse[ledger.Transaction](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := api.ErrorResponse{}
				api.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)

			}
		})
	}
}
