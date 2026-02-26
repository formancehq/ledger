package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/require"
)

func TestHandleDeleteAccountMetadata_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
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

	srv := newTestServer(t, &mockBackend{})

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

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrMetadataNotFound{Target: "account:users:001", Key: "role"}
		},
	}
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
