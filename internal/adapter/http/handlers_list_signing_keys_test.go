package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
)

func TestHandleListSigningKeys_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListSigningKeys(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*ledgerpb.SigningKey], error) {
			return cursor.NewSliceCursor([]*ledgerpb.SigningKey{
				{KeyId: "k1"},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/signing-keys", nil, nil)

	srv.handleListSigningKeys(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListSigningKeys_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListSigningKeys(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*ledgerpb.SigningKey], error) {
			return nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/signing-keys", nil, nil)

	srv.handleListSigningKeys(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
