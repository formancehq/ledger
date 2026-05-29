package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetLedgerStats_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, ledgerName string) (*commonpb.LedgerStats, error) {
			require.Equal(t, "my-ledger", ledgerName)

			return &commonpb.LedgerStats{
				TransactionCount: 100,
				VolumeCount:      42,
				MetadataCount:    10,
				ReferenceCount:   5,
				PostingCount:     200,
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/stats", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[ledgerStatsJSON]](t, w)
	require.Equal(t, uint64(100), wrapper.Data.TransactionCount)
	require.Equal(t, uint64(42), wrapper.Data.VolumeCount)
	require.Equal(t, uint64(10), wrapper.Data.MetadataCount)
	require.Equal(t, uint64(5), wrapper.Data.ReferenceCount)
	require.Equal(t, uint64(200), wrapper.Data.PostingCount)
}

func TestHandleGetLedgerStats_EmptyLedger(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return &commonpb.LedgerStats{}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/empty-ledger/stats", nil, map[string]string{
		"ledgerName": "empty-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[ledgerStatsJSON]](t, w)
	require.Equal(t, uint64(0), wrapper.Data.TransactionCount)
	require.Equal(t, uint64(0), wrapper.Data.VolumeCount)
	require.Equal(t, uint64(0), wrapper.Data.MetadataCount)
	require.Equal(t, uint64(0), wrapper.Data.ReferenceCount)
	require.Equal(t, uint64(0), wrapper.Data.PostingCount)
}

func TestHandleGetLedgerStats_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/stats", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetLedgerStats_BackendError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return nil, errors.New("internal error")
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/stats", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleGetLedgerStats_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/stats", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetLedgerStats_NoLeaderError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return nil, commonpb.ErrNoLeader
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/stats", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleGetLedgerStats(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestHandleGetLedgerStats_FullRouteIntegration tests that the route is correctly
// registered in NewHandler and accessible via a full HTTP request.
func TestHandleGetLedgerStats_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getLedgerStatsFn: func(_ context.Context, _ string) (*commonpb.LedgerStats, error) {
			return &commonpb.LedgerStats{
				TransactionCount: 10,
				VolumeCount:      5,
			}, nil
		},
	}

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/my-ledger/stats", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}
