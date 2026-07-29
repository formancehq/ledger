package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListAllLedgers_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
			return cursor.NewSliceCursor([]*commonpb.LedgerInfo{
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

func TestHandleListAllLedgers_RedactsSecrets(t *testing.T) {
	t.Parallel()

	original := &commonpb.LedgerInfo{
		Name: "mirror",
		MirrorSource: &commonpb.MirrorSourceConfig{
			Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{
				Dsn: "postgres://user:http-list-secret@db.example/ledger?sslmode=require",
			}},
		},
	}
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).Return(cursor.NewSliceCursor([]*commonpb.LedgerInfo{original}), nil)
	srv := newTestServer(t, backend)
	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, nil)

	srv.handleListAllLedgers(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "http-list-secret")
	require.Contains(t, w.Body.String(), "db.example/ledger")
	require.Contains(t, original.GetMirrorSource().GetPostgres().GetDsn(), "http-list-secret")
}

func TestHandleListAllLedgers_Empty(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListLedgers(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
			return cursor.NewSliceCursor[*commonpb.LedgerInfo](nil), nil
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
		func(_ context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/", nil, nil)

	srv.handleListAllLedgers(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
