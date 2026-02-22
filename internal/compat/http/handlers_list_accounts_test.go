package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
)

func TestHandleListAccounts_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		listAccountsFn: func(_ context.Context, _ string, _ uint32, _ string, _ string) (dal.Cursor[*commonpb.Account], error) {
			return dal.NewSliceCursor([]*commonpb.Account{
				{Address: "users:001"},
				{Address: "users:002"},
			}), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAccounts_WithPagination(t *testing.T) {
	t.Parallel()

	var capturedPageSize uint32
	var capturedAfter string
	backend := &mockBackend{
		listAccountsFn: func(_ context.Context, _ string, pageSize uint32, afterAddress string, _ string) (dal.Cursor[*commonpb.Account], error) {
			capturedPageSize = pageSize
			capturedAfter = afterAddress
			return dal.NewSliceCursor[*commonpb.Account](nil), nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?pageSize=10&after=users:005", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, uint32(10), capturedPageSize)
	require.Equal(t, "users:005", capturedAfter)
}

func TestHandleListAccounts_InvalidPageSize(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?pageSize=abc", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListAccounts_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/accounts", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
