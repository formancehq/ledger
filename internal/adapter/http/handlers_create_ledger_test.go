package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleCreateLedger_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_CreateLedger{
							CreateLedger: &commonpb.CreatedLedgerLog{
								Name: "test-ledger",
							},
						},
					},
				},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleCreateLedger_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateLedger_AlreadyExists(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerAlreadyExists{Name: "test-ledger"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "CONFLICT", resp.ErrorCode)
}
