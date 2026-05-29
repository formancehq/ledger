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

func TestHandleCreateTransaction_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
											CreatedTransaction: &commonpb.CreatedTransaction{
												Transaction: &commonpb.Transaction{
													Id: 1,
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
	body := strings.NewReader(`{"script":{"plain":"send [USD 100] (\n  source = @world\n  destination = @users:001\n)"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleCreateTransaction_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{invalid`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateTransaction_InsufficientFunds(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrInsufficientFunds{
				Account: "users:001",
				Asset:   "USD",
				Amount:  "100",
				Balance: "50",
			}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"script":{"plain":"send [USD 100] (\n  source = @users:001\n  destination = @users:002\n)"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INSUFFICIENT_FUNDS", resp.ErrorCode)
}

func TestHandleCreateTransaction_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{}`)
	r := newRequest(t, http.MethodPost, "/transactions", body, map[string]string{
		"ledgerName": "",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
