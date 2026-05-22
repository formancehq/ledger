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

func TestHandlePromoteLedger_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_PromoteLedger{
						PromoteLedger: &commonpb.PromotedLedgerLog{
							Name: "mirror-ledger",
						},
					},
				},
			}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-ledger/promote", nil, map[string]string{
		"ledgerName": "mirror-ledger",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandlePromoteLedger_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/promote", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandlePromoteLedger_NotMirrorMode(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerNotInMirrorMode{Name: "normal-ledger"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/normal-ledger/promote", nil, map[string]string{
		"ledgerName": "normal-ledger",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "LEDGER_NOT_IN_MIRROR_MODE", resp.ErrorCode)
}

func TestHandlePromoteLedger_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/missing/promote", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
