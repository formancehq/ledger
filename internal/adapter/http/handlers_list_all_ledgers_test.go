package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	protoerr "github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListAllLedgers_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*ledgerpb.LedgerInfo], error) {
			return cursor.NewSliceCursor([]*ledgerpb.LedgerInfo{
				{Name: "ledger-a"},
				{Name: "ledger-b"},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, nil)

	srv.handleListAllLedgers(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAllLedgers_Empty(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*ledgerpb.LedgerInfo], error) {
			return cursor.NewSliceCursor[*ledgerpb.LedgerInfo](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, nil)

	srv.handleListAllLedgers(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListAllLedgers_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*ledgerpb.LedgerInfo], error) {
			return nil, protoerr.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, nil)

	srv.handleListAllLedgers(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
