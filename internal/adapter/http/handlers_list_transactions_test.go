package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

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

// TestHandleListTransactions_ReferenceFilterAndDateRange proves the canonical
// replacement for the removed `reference=` alias: a reference selection passed
// through the generic `filter` as the structured `{"$match":{"reference":...}}`
// is AND-combined with the startDate/endDate timestamp range, exactly as the old
// alias was. The removed alias must no longer be interpreted.
func TestHandleListTransactions_ReferenceFilterAndDateRange(t *testing.T) {
	t.Parallel()

	capture := func(t *testing.T, target string) *commonpb.QueryFilter {
		t.Helper()

		var capturedFilter *commonpb.QueryFilter

		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().ListTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, _ uint32, _ uint64, filter *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Transaction], error) {
				capturedFilter = filter

				return cursor.NewSliceCursor[*commonpb.Transaction](nil), nil
			}).AnyTimes()
		srv := newTestServer(t, backend)

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, target, nil, map[string]string{"ledgerName": "ledger1"})
		srv.handleListTransactions(w, r)

		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		return capturedFilter
	}

	// Canonical reference selection via the structured `filter`, AND-combined
	// with the date range → a 2-element $and (date range + reference).
	refFilter := url.QueryEscape(`{"$match":{"reference":"ref-1"}}`)
	combined := capture(t, "/ledger1/transactions?filter="+refFilter+
		"&startDate=2026-01-01T00:00:00Z&endDate=2026-02-01T00:00:00Z")
	require.NotNil(t, combined)
	and := combined.GetAnd()
	require.NotNil(t, and, "reference filter + date range must AND-combine")
	require.Len(t, and.GetFilters(), 2)

	// The reference selection reaches the backend as a ReferenceCondition, i.e.
	// the same QueryFilter the removed `reference=` alias produced. Locate the
	// non-date sub-filter and assert its shape.
	var refCond *commonpb.QueryFilter
	for _, f := range and.GetFilters() {
		if f.GetBuiltinUint() == nil {
			refCond = f
		}
	}
	require.NotNil(t, refCond, "reference sub-filter must be present alongside the date range")
	require.Equal(t, "ref-1", refCond.GetReference().GetCond().GetHardcoded())

	// The removed `reference=` alias must no longer be interpreted: passed alone
	// (no `filter=`), it yields an unfiltered read (nil filter), not a reference
	// selection.
	aliasOnly := capture(t, "/ledger1/transactions?reference=ref-1")
	require.Nil(t, aliasOnly, "the removed reference= alias must not build a filter")
}

// TestHandleListTransactions_DualFormatFilter is the endpoint-level EN-1511
// acceptance check: the same logical filter passed via `?filter=` in the textual
// form and in the structured JSON form reaches the backend as the same
// QueryFilter.
func TestHandleListTransactions_DualFormatFilter(t *testing.T) {
	t.Parallel()

	capture := func(t *testing.T, target string) *commonpb.QueryFilter {
		t.Helper()

		var captured *commonpb.QueryFilter

		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().ListTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, _ uint32, _ uint64, filter *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Transaction], error) {
				captured = filter

				return cursor.NewSliceCursor[*commonpb.Transaction](nil), nil
			}).AnyTimes()
		srv := newTestServer(t, backend)

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, target, nil, map[string]string{"ledgerName": "ledger1"})
		srv.handleListTransactions(w, r)

		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
		require.NotNil(t, captured)

		return captured
	}

	// url.QueryEscape both values so the JSON braces / spaces survive the query
	// string.
	fromText := capture(t, "/ledger1/transactions?filter="+url.QueryEscape(`metadata[status] == "active"`))
	fromJSON := capture(t, "/ledger1/transactions?filter="+url.QueryEscape(`{"$match":{"metadata[status]":"active"}}`))

	require.True(t, proto.Equal(fromText, fromJSON),
		"textual and JSON ?filter= forms must reach the backend as the same QueryFilter\n text: %v\n json: %v",
		fromText, fromJSON)
}

// TestHandleListTransactions_FilterInvalidForTarget checks that a condition
// invalid on the transactions target is rejected with a 400 for both forms.
func TestHandleListTransactions_FilterInvalidForTarget(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{`ledger == "main"`, `{"$match":{"ledger":"main"}}`} {
		srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, "/ledger1/transactions?filter="+url.QueryEscape(raw), nil,
			map[string]string{"ledgerName": "ledger1"})
		srv.handleListTransactions(w, r)

		require.Equal(t, http.StatusBadRequest, w.Code, "raw: %s", raw)
	}
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
