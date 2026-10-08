package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestHandleGetLedger_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string) (*ledgerpb.LedgerInfo, error) {
			return &ledgerpb.LedgerInfo{Name: name}, nil
		}).AnyTimes()
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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleGetLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetLedger_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*ledgerpb.LedgerInfo, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleGetLedger(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
