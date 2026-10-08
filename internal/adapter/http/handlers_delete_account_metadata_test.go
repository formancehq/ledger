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

func TestHandleDeleteAccountMetadata_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
			return &domain.ApplyResult{Logs: []*ledgerpb.Log{{}}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/accounts/users:001/metadata/role", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
		"key":        "role",
	})

	srv.handleDeleteAccountMetadata(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleDeleteAccountMetadata_MissingKey(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/accounts/users:001/metadata/", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
		"key":        "",
	})

	srv.handleDeleteAccountMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteAccountMetadata_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
			return nil, &domain.ErrMetadataNotFound{Target: "account:users:001", Key: "role"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/accounts/users:001/metadata/role", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
		"key":        "role",
	})

	srv.handleDeleteAccountMetadata(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
