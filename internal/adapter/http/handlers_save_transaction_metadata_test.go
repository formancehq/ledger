package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestHandleSaveTransactionMetadata_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
			return &domain.ApplyResult{Logs: []*ledgerpb.Log{{}}}, nil
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
