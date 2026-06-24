package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleSaveTransactionMetadata_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"key":"val"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/abc/metadata", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
	})

	srv.handleSaveTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
