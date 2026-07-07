package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetTransaction_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, *string, error) {
			return &commonpb.Transaction{Id: txID}, nil, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "42",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetTransaction_RevertRelationshipFields(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	// Transaction 1 was reverted by transaction 2, which in turn reverts 1.
	backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, *string, error) {
			if txID == 1 {
				return &commonpb.Transaction{
					Id:                    1,
					Reverted:              true,
					RevertedByTransaction: 2,
					RevertedAt:            &commonpb.Timestamp{Data: 1_700_000_000_000_000},
				}, nil, nil
			}

			return &commonpb.Transaction{Id: 2, RevertsTransaction: 1}, nil, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	// The reverted original exposes the forward link and reverted_at.
	wOrig := httptest.NewRecorder()
	srv.handleGetTransaction(wOrig, newRequest(t, http.MethodGet, "/ledger1/transactions/1", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	}))
	require.Equal(t, http.StatusOK, wOrig.Code)
	require.Contains(t, wOrig.Body.String(), `"reverted":true`)
	require.Contains(t, wOrig.Body.String(), `"revertedByTransactionId":2`)
	require.Contains(t, wOrig.Body.String(), `"revertedAt":`)

	// The compensating transaction exposes the back link.
	wRevert := httptest.NewRecorder()
	srv.handleGetTransaction(wRevert, newRequest(t, http.MethodGet, "/ledger1/transactions/2", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "2",
	}))
	require.Equal(t, http.StatusOK, wRevert.Code)
	require.Contains(t, wRevert.Body.String(), `"revertsTransactionId":1`)
}

func TestHandleGetTransaction_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/abc", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetTransaction_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint64) (*commonpb.Transaction, *string, error) {
			return nil, nil, &domain.ErrTransactionNotFound{TransactionID: 999}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/999", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "999",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetTransaction_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, map[string]string{
		"ledgerName":    "",
		"transactionId": "1",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
