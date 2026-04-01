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

func TestHandleCreatePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries", strings.NewReader(`{"name":"my-query","target":"ACCOUNTS"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleCreatePreparedQuery_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/prepared-queries", strings.NewReader(`{"name":"my-query"}`), map[string]string{
		"ledgerName": "",
	})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries", strings.NewReader(`{"target":"ACCOUNTS"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries", strings.NewReader(`not-json`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreatePreparedQuery_AlreadyExists(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrPreparedQueryAlreadyExists{Ledger: "ledger1", Name: "my-query"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/prepared-queries", strings.NewReader(`{"name":"my-query","target":"TRANSACTIONS"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreatePreparedQuery(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
}
