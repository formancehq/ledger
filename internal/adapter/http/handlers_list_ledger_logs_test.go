package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListLedgerLogs_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listLogsFn: func(_ context.Context, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor([]*commonpb.Log{
				{Sequence: 1},
				{Sequence: 2},
			}), nil
		},
	}
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

	backend := &mockBackend{
		listLogsFn: func(_ context.Context, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor[*commonpb.Log](nil), nil
		},
	}
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

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidPageSize(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?pageSize=abc", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidAfter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?after=notanumber", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidStartDate(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?startDate=not-a-date", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_InvalidEndDate(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?endDate=not-a-date", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerLogs_WithDateFilters(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listLogsFn: func(_ context.Context, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor[*commonpb.Log](nil), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?startDate=2024-01-01T00:00:00Z&endDate=2024-12-31T23:59:59Z", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListLedgerLogs_WithAfterParam(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listLogsFn: func(_ context.Context, _ uint64, _ uint32, _ *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
			return cursor.NewSliceCursor[*commonpb.Log](nil), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/logs?after=42&pageSize=10", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerLogs(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}
