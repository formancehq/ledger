package v1_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	ledger "github.com/formancehq/ledger/internal"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	"github.com/formancehq/ledger/internal/engine"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/ledger/internal/storage/query"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/migrations"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetLedgerInfo(t *testing.T) {
	t.Parallel()

	backend, mock := newTestingBackend(t, false)
	router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	migrationInfo := []migrations.Info{
		{
			Version: "1",
			Name:    "init",
			State:   "ready",
			Date:    time.Now().Add(-2 * time.Minute).Round(time.Second).UTC(),
		},
		{
			Version: "2",
			Name:    "fix",
			State:   "ready",
			Date:    time.Now().Add(-time.Minute).Round(time.Second).UTC(),
		},
	}

	mock.EXPECT().
		GetMigrationsInfo(gomock.Any()).
		Return(migrationInfo, nil)

	req := httptest.NewRequest(http.MethodGet, "/xxx/_info", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	info, ok := sharedapi.DecodeSingleResponse[v1.Info](t, rec.Body)
	require.True(t, ok)

	require.EqualValues(t, v1.Info{
		Name: "xxx",
		Storage: v1.StorageInfo{
			Migrations: migrationInfo,
		},
	}, info)
}

func TestGetStats(t *testing.T) {
	t.Parallel()

	backend, mock := newTestingBackend(t, true)
	router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	expectedStats := engine.Stats{
		Transactions: 10,
		Accounts:     5,
	}

	mock.EXPECT().
		Stats(gomock.Any()).
		Return(expectedStats, nil)

	req := httptest.NewRequest(http.MethodGet, "/xxx/stats", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	stats, ok := sharedapi.DecodeSingleResponse[engine.Stats](t, rec.Body)
	require.True(t, ok)

	require.EqualValues(t, expectedStats, stats)
}

func TestGetLogs(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgerstore.PaginatedQueryOptions[any]
		expectStatusCode  int
		expectedErrorCode string
	}

	now := ledger.Now()
	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewPaginatedQueryOptions[any](nil),
		},
		{
			name: "using start time",
			queryParams: url.Values{
				"start_time": []string{now.Format(ledger.DateFormat)},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions[any](nil).WithQueryBuilder(query.Gte("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using end time",
			queryParams: url.Values{
				"end_time": []string{now.Format(ledger.DateFormat)},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions[any](nil).
				WithQueryBuilder(query.Lt("date", now.Format(ledger.DateFormat))),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{paginate.EncodeCursor(ledgerstore.NewGetLogsQuery(ledgerstore.NewPaginatedQueryOptions[any](nil)))},
			},
			expectQuery: ledgerstore.NewPaginatedQueryOptions[any](nil),
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: v1.ErrValidation,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := sharedapi.Cursor[ledger.ChainedLog]{
				Data: []ledger.ChainedLog{
					*ledger.NewTransactionLog(ledger.NewTransaction(), map[string]metadata.Metadata{}).
						ChainLog(nil),
				},
			}

			backend, mockLedger := newTestingBackend(t, true)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetLogs(gomock.Any(), ledgerstore.NewGetLogsQuery(testCase.expectQuery)).
					Return(&expectedCursor, nil)
			}

			router := v1.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/logs", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := sharedapi.DecodeCursorResponse[ledger.ChainedLog](t, rec.Body)

				cursorData, err := json.Marshal(cursor)
				require.NoError(t, err)

				cursorAsMap := make(map[string]any)
				require.NoError(t, json.Unmarshal(cursorData, &cursorAsMap))

				expectedCursorData, err := json.Marshal(expectedCursor)
				require.NoError(t, err)

				expectedCursorAsMap := make(map[string]any)
				require.NoError(t, json.Unmarshal(expectedCursorData, &expectedCursorAsMap))

				require.Equal(t, expectedCursorAsMap, cursorAsMap)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
