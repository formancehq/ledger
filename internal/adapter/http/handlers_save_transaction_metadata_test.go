package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleSaveTransactionMetadata_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"category":"refund"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/metadata", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSaveTransactionMetadata_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{invalid`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/metadata", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveTransactionMetadata_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"key":"val"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/abc/metadata", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
