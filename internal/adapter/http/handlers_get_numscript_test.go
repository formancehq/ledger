package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestHandleGetNumscript_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getNumscriptFn: func(_ context.Context, _, _ string, _ string) (*commonpb.NumscriptInfo, error) {
			return &commonpb.NumscriptInfo{
				Name:    "my-script",
				Content: "send [USD 100] ( source = @world destination = @alice )",
				Version: "1.0.0",
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts/my-script", nil, map[string]string{
		"ledgerName": "ledger1",
		"name":       "my-script",
	})

	srv.handleGetNumscript(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetNumscript_WithVersion(t *testing.T) {
	t.Parallel()

	var capturedVersion string

	backend := &mockBackend{
		getNumscriptFn: func(_ context.Context, _, _ string, version string) (*commonpb.NumscriptInfo, error) {
			capturedVersion = version

			return &commonpb.NumscriptInfo{
				Name:    "my-script",
				Version: "1.0.0",
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts/my-script?version=1.0.0", nil, map[string]string{
		"ledgerName": "ledger1",
		"name":       "my-script",
	})

	srv.handleGetNumscript(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "1.0.0", capturedVersion)
}

func TestHandleGetNumscript_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/numscripts/my-script", nil, map[string]string{
		"ledgerName": "",
		"name":       "my-script",
	})

	srv.handleGetNumscript(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetNumscript_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts/", nil, map[string]string{
		"ledgerName": "ledger1",
		"name":       "",
	})

	srv.handleGetNumscript(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetNumscript_NotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		getNumscriptFn: func(_ context.Context, _, _ string, _ string) (*commonpb.NumscriptInfo, error) {
			return nil, &domain.ErrNumscriptNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts/missing", nil, map[string]string{
		"ledgerName": "ledger1",
		"name":       "missing",
	})

	srv.handleGetNumscript(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
