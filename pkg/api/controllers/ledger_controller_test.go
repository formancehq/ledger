package controllers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetLedgerInfo(t *testing.T) {
	t.Parallel()

	backend, mock := newTestingBackend(t)
	router := routes.NewRouter(backend, nil, nil, metrics.NewNoOpMetricsRegistry())

	migrationInfo := []core.MigrationInfo{
		{
			Version: "1",
			Name:    "init",
			State:   "ready",
			Date:    core.Now().Add(-2 * time.Minute).Round(time.Second),
		},
		{
			Version: "2",
			Name:    "fix",
			State:   "ready",
			Date:    core.Now().Add(-time.Minute).Round(time.Second),
		},
	}

	mock.EXPECT().
		GetMigrationsInfo(gomock.Any()).
		Return(migrationInfo, nil)

	req := httptest.NewRequest(http.MethodGet, "/xxx/_info", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	info, ok := DecodeSingleResponse[controllers.Info](t, rec.Body)
	require.True(t, ok)

	require.EqualValues(t, controllers.Info{
		Name: "xxx",
		Storage: controllers.StorageInfo{
			Migrations: migrationInfo,
		},
	}, info)
}

func TestGetStats(t *testing.T) {
	t.Parallel()

	backend, mock := newTestingBackend(t)
	router := routes.NewRouter(backend, nil, nil, metrics.NewNoOpMetricsRegistry())

	expectedStats := ledger.Stats{
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

	stats, ok := DecodeSingleResponse[ledger.Stats](t, rec.Body)
	require.True(t, ok)

	require.EqualValues(t, expectedStats, stats)
}

func TestGetLogs(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgerstore.LogsQuery
		expectStatusCode  int
		expectedErrorCode string
	}

	now := core.Now()
	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewLogsQuery(),
		},
		{
			name: "using start time",
			queryParams: url.Values{
				"startTime": []string{now.Format(core.DateFormat)},
			},
			expectQuery: ledgerstore.NewLogsQuery().WithStartTimeFilter(now),
		},
		{
			name: "using end time",
			queryParams: url.Values{
				"endTime": []string{now.Format(core.DateFormat)},
			},
			expectQuery: ledgerstore.NewLogsQuery().WithEndTimeFilter(now),
		},
		{
			name: "using invalid start time",
			queryParams: url.Values{
				"startTime": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using invalid end time",
			queryParams: url.Values{
				"endTime": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{ledgerstore.EncodeCursor(ledgerstore.NewLogsQuery())},
			},
			expectQuery: ledgerstore.NewLogsQuery(),
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"xxx"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := sharedapi.Cursor[core.PersistedLog]{
				Data: []core.PersistedLog{
					*core.NewTransactionLog(core.Transaction{}, map[string]metadata.Metadata{}).
						ComputePersistentLog(nil),
				},
			}

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetLogs(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, nil)
			}

			router := routes.NewRouter(backend, nil, nil, metrics.NewNoOpMetricsRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/logs", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := DecodeCursorResponse[core.PersistedLog](t, rec.Body)

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
				Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
