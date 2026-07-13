package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListLedgerLogs_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor([]*commonpb.Log{
				{Sequence: 1},
				{Sequence: 2},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListLedgerLogs_Empty(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor[*commonpb.Log](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListLedgerLogs_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidPageSize(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?pageSize=abc", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidAfter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?after=notanumber", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidStartDate(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?startDate=not-a-date", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidEndDate(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?endDate=not-a-date", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_WithDateFilters(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor[*commonpb.Log](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?startDate=2024-01-01T00:00:00Z&endDate=2024-12-31T23:59:59Z", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// TestHandleListLedgerLogs_DateBounds is the EN-1542 acceptance suite: it
// validates that pre-epoch and malformed startDate/endDate values are rejected
// with a 400 BEFORE the backend is invoked, and that epoch / post-epoch values
// produce exact microsecond bounds with start-inclusive / end-exclusive
// semantics. It shares parseFilterDateMicros with handleListTransactions.
func TestHandleListLedgerLogs_DateBounds(t *testing.T) {
	t.Parallel()

	// mustMicros mirrors the handler's RFC3339 -> unsigned micros conversion so
	// the expected bounds below are derived from the same source of truth.
	mustMicros := func(rfc3339 string) uint64 {
		ts, err := time.Parse(time.RFC3339, rfc3339)
		require.NoError(t, err)

		return uint64(ts.UnixMicro())
	}

	type expectBound struct {
		min          *uint64
		max          *uint64
		maxExclusive bool
	}

	for _, tc := range []struct {
		name string
		// query is the raw query string (without the leading '?').
		query string
		// wantStatus is the expected HTTP status.
		wantStatus int
		// wantBackend is whether the backend is expected to be invoked. On a
		// validation error it MUST NOT be called.
		wantBackend bool
		// wantBodyContains, when non-empty, is asserted against the error body.
		wantBodyContains string
		// bound, when wantBackend is true, is the expected date UintCondition.
		bound *expectBound
	}{
		{
			name:             "pre-epoch startDate rejected",
			query:            "startDate=1960-01-01T00:00:00Z",
			wantStatus:       http.StatusBadRequest,
			wantBackend:      false,
			wantBodyContains: "startDate parameter, dates before 1970-01-01",
		},
		{
			name:             "pre-epoch endDate rejected",
			query:            "endDate=1969-12-31T23:59:59Z",
			wantStatus:       http.StatusBadRequest,
			wantBackend:      false,
			wantBodyContains: "endDate parameter, dates before 1970-01-01",
		},
		{
			name:             "malformed startDate rejected",
			query:            "startDate=not-a-date",
			wantStatus:       http.StatusBadRequest,
			wantBackend:      false,
			wantBodyContains: "startDate parameter, expected RFC3339",
		},
		{
			name:             "malformed endDate rejected",
			query:            "endDate=2024-13-99",
			wantStatus:       http.StatusBadRequest,
			wantBackend:      false,
			wantBodyContains: "endDate parameter, expected RFC3339",
		},
		{
			name:        "epoch startDate is an exact inclusive bound",
			query:       "startDate=1970-01-01T00:00:00Z",
			wantStatus:  http.StatusOK,
			wantBackend: true,
			bound: &expectBound{
				min: new(mustMicros("1970-01-01T00:00:00Z")), // == 0
			},
		},
		{
			name:        "post-epoch startDate is an exact inclusive bound",
			query:       "startDate=2024-01-01T00:00:00Z",
			wantStatus:  http.StatusOK,
			wantBackend: true,
			bound: &expectBound{
				min: new(mustMicros("2024-01-01T00:00:00Z")),
			},
		},
		{
			name:        "post-epoch endDate is an exact exclusive bound",
			query:       "endDate=2024-12-31T23:59:59Z",
			wantStatus:  http.StatusOK,
			wantBackend: true,
			bound: &expectBound{
				max:          new(mustMicros("2024-12-31T23:59:59Z")),
				maxExclusive: true,
			},
		},
		{
			name:        "range is start-inclusive end-exclusive",
			query:       "startDate=2024-01-01T00:00:00Z&endDate=2024-12-31T23:59:59Z",
			wantStatus:  http.StatusOK,
			wantBackend: true,
			bound: &expectBound{
				min:          new(mustMicros("2024-01-01T00:00:00Z")),
				max:          new(mustMicros("2024-12-31T23:59:59Z")),
				maxExclusive: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var capturedFilter *commonpb.QueryFilter

			backend := NewMockBackend(gomock.NewController(t))
			if tc.wantBackend {
				backend.EXPECT().ListLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ string, _ uint64, _ uint32, filter *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
						capturedFilter = filter

						return cursor.NewSliceCursor[*commonpb.Log](nil), nil
					}).Times(1)
			} else {
				// On a validation error the backend must never be reached: any
				// call fails the test.
				backend.EXPECT().ListLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			}

			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/ledger1/logs?"+tc.query, nil, map[string]string{
				"ledgerName": "ledger1",
			})

			srv.handleListLedgerLogs(w, r)

			require.Equal(t, tc.wantStatus, w.Code, "body: %s", w.Body.String())

			if tc.wantBodyContains != "" {
				require.Contains(t, w.Body.String(), tc.wantBodyContains)
			}

			if tc.bound == nil {
				return
			}

			require.NotNil(t, capturedFilter)
			cond := capturedFilter.GetLogBuiltinUint()
			require.NotNil(t, cond, "expected a LogBuiltinUint date condition")
			require.Equal(t, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, cond.GetField())

			uc := cond.GetCond()
			require.NotNil(t, uc)

			if tc.bound.min != nil {
				require.NotNil(t, uc.Min)
				require.Equal(t, *tc.bound.min, uc.GetMin())
				// start bound is inclusive
				require.False(t, uc.GetMinExclusive())
			} else {
				require.Nil(t, uc.Min)
			}

			if tc.bound.max != nil {
				require.NotNil(t, uc.Max)
				require.Equal(t, *tc.bound.max, uc.GetMax())
				require.Equal(t, tc.bound.maxExclusive, uc.GetMaxExclusive())
			} else {
				require.Nil(t, uc.Max)
			}
		})
	}
}

func TestHandleListLedgerLogs_WithAfterParam(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor[*commonpb.Log](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?after=42&pageSize=10", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}
