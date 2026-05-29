package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetTransaction_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		},
		getTransactionFn: func(_ context.Context, _ string, txID uint64) (*commonpb.Transaction, error) {
			return &commonpb.Transaction{Id: txID}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/transactions/42", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "42",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetTransaction_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

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

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		},
		getTransactionFn: func(_ context.Context, _ string, _ uint64) (*commonpb.Transaction, error) {
			return nil, &domain.ErrTransactionNotFound{TransactionID: 999}
		},
	}
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

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, map[string]string{
		"ledgerName":    "",
		"transactionId": "1",
	})

	srv.handleGetTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
