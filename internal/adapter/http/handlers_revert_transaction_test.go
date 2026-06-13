package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleRevertTransaction_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{
													Id: 2,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleRevertTransaction_AlreadyReverted(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrTransactionAlreadyReverted{TransactionID: 1}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "TRANSACTION_ALREADY_REVERTED", resp.ErrorCode)
}

func TestHandleRevertTransaction_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/abc/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRevertTransaction_WithBody(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{
													Id: 2,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"force": true, "atEffectiveDate": true}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.Header.Set("Content-Length", "42")

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}
