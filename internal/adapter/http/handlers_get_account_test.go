package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetAccount_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, addr string) (*commonpb.Account, error) {
			return &commonpb.Account{Address: addr}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/users:001", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetAccount_MissingAddress(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetAccount_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/accounts/addr", nil, map[string]string{
		"ledgerName": "missing",
		"address":    "addr",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
