package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleUpdatePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(`{"filter":{}}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleUpdatePreparedQuery_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/prepared-queries/my-query",
		strings.NewReader(`{"filter":{}}`),
		map[string]string{
			"ledgerName": "",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_MissingQueryName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/",
		strings.NewReader(`{"filter":{}}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(`not-json`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "my-query",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleUpdatePreparedQuery_NotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrPreparedQueryNotFound{Ledger: "ledger1", Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/missing",
		strings.NewReader(`{"filter":{}}`),
		map[string]string{
			"ledgerName": "ledger1",
			"queryName":  "missing",
		})

	srv.handleUpdatePreparedQuery(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
