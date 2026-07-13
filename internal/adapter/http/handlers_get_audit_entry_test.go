package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetAuditEntry_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetAuditEntry(gomock.Any(), uint64(7)).DoAndReturn(
		func(_ context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
			require.EqualValues(t, 7, sequence)

			return &auditpb.AuditEntry{
				Sequence:   7,
				OrderCount: 1,
				Items: []*auditpb.AuditItem{
					{OrderIndex: 0, LogSequence: 12},
				},
			}, nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries/7", nil, map[string]string{
		"sequence": "7",
	})

	srv.handleGetAuditEntry(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	resp := decodeResponse[BaseResponse[map[string]any]](t, w)
	require.EqualValues(t, 7, resp.Data["sequence"])
	// items are populated on the single-entry lookup (camelCase key).
	items, ok := resp.Data["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
}

func TestHandleGetAuditEntry_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetAuditEntry(gomock.Any(), uint64(99)).DoAndReturn(
		func(_ context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
			return nil, commonpb.NewNotFoundError("audit entry %d not found", sequence)
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries/99", nil, map[string]string{
		"sequence": "99",
	})

	srv.handleGetAuditEntry(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetAuditEntry_InvalidSequence(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries/notanumber", nil, map[string]string{
		"sequence": "notanumber",
	})

	srv.handleGetAuditEntry(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetAuditEntry_MissingSequence(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/audit-entries/", nil, map[string]string{
		"sequence": "",
	})

	srv.handleGetAuditEntry(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestAuditRoutes_FullRouteIntegration verifies both audit routes are registered
// in NewHandler and that the static /_/audit-entries segment is matched ahead of
// the /{ledgerName} wildcard (so audit reads are not swallowed by the ledger
// routes).
func TestAuditRoutes_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			return cursor.NewSliceCursor([]*auditpb.AuditEntry{{Sequence: 1}}), nil
		}).AnyTimes()
	backend.EXPECT().GetAuditEntry(gomock.Any(), uint64(1)).DoAndReturn(
		func(_ context.Context, _ uint64) (*auditpb.AuditEntry, error) {
			return &auditpb.AuditEntry{Sequence: 1}, nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	t.Run("list", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/v3/_/audit-entries", nil)
		handler.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("get", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/v3/_/audit-entries/1", nil)
		handler.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code)
	})
}
