package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/require"
)

func TestHandleDeleteLedger_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/my-ledger", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleDeleteLedger(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleDeleteLedger_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleDeleteLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteLedger_NotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/missing", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleDeleteLedger(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
