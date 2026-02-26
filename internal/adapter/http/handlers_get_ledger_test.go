package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
)

func TestHandleGetLedger_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: name}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleGetLedger(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetLedger_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleGetLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetLedger_NotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerByNameFn: func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return nil, errors.New("ledger not found")
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleGetLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
