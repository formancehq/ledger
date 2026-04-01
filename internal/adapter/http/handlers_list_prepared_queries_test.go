package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestHandleListPreparedQueries_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listPreparedQueriesFn: func(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
			return []*commonpb.PreparedQuery{
				{Name: "query1", Ledger: "ledger1"},
				{Name: "query2", Ledger: "ledger1"},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/prepared-queries", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListPreparedQueries_Empty(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listPreparedQueriesFn: func(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
			return nil, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/prepared-queries", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListPreparedQueries_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/prepared-queries", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListPreparedQueries_BackendError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listPreparedQueriesFn: func(_ context.Context, _ string) ([]*commonpb.PreparedQuery, error) {
			return nil, fmt.Errorf("unexpected error")
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/prepared-queries", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListPreparedQueries(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
