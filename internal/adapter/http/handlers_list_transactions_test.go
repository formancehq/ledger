package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListTransactions_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Transaction], error) {
			return cursor.NewSliceCursor([]*commonpb.Transaction{
				{Id: 1, RevertedByTransaction: 7},
				{Id: 2},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// Transactions must serialize in protobuf-JSON camelCase (revertedByTransaction)
	// inside the {data:[...]} envelope — not the snake_case Go struct tags
	// (reverted_by_transaction) a plain sonic marshal would emit. Parity with
	// the sibling proto-returning handlers (see writeProtoListOK).
	body := w.Body.String()
	require.Contains(t, body, `"revertedByTransaction":"7"`)
	require.NotContains(t, body, "reverted_by_transaction")
}

func TestHandleListTransactions_WithPaginationAndReverse(t *testing.T) {
	t.Parallel()

	var (
		capturedPageSize uint32
		capturedAfter    uint64
		capturedReverse  bool
	)

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, pageSize uint32, afterTxID uint64, _ *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error) {
			capturedPageSize = pageSize
			capturedAfter = afterTxID
			capturedReverse = reverse

			return cursor.NewSliceCursor[*commonpb.Transaction](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions?pageSize=25&after=42&reverse=true", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, uint32(25), capturedPageSize)
	require.Equal(t, uint64(42), capturedAfter)
	require.True(t, capturedReverse)
}

func TestHandleListTransactions_WithReferenceAndDateFilters(t *testing.T) {
	t.Parallel()

	var capturedFilter *commonpb.QueryFilter

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ uint64, filter *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Transaction], error) {
			capturedFilter = filter

			return cursor.NewSliceCursor[*commonpb.Transaction](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet,
		"/ledger1/transactions?reference=ref-1&startDate=2026-01-01T00:00:00Z&endDate=2026-02-01T00:00:00Z",
		nil, map[string]string{"ledgerName": "ledger1"})

	srv.handleListTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedFilter)
	// reference + date range → wrapped in QueryFilter_And
	and := capturedFilter.GetAnd()
	require.NotNil(t, and)
	require.Len(t, and.GetFilters(), 2)
}

func TestHandleListTransactions_InvalidAfter(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions?after=notanumber", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListTransactions(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListTransactions_InvalidDate(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions?startDate=not-a-date", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListTransactions(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListTransactions_PreEpochDateRejected(t *testing.T) {
	t.Parallel()

	// A pre-1970 date has a negative UnixMicro; the storage bound is unsigned,
	// so accepting it would wrap to a huge value and silently corrupt the
	// filter. It must be rejected with 400 (both startDate and endDate).
	for _, param := range []string{"startDate", "endDate"} {
		t.Run(param, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/ledger1/transactions?"+param+"=1960-01-01T00:00:00Z", nil, map[string]string{
				"ledgerName": "ledger1",
			})

			srv.handleListTransactions(w, r)

			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "before 1970-01-01")
		})
	}
}

func TestHandleListTransactions_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/transactions", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListTransactions(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListTransactions_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Transaction], error) {
			return nil, errors.New("backend broke")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListTransactions(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "INTERNAL_ERROR")
}
