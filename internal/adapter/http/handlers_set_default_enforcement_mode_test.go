package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleSetDefaultEnforcementMode_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/account-types/default-enforcement-mode",
		strings.NewReader(`{"enforcementMode":"STRICT"}`),
		map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleSetDefaultEnforcementMode(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSetDefaultEnforcementMode_AuditMode(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/account-types/default-enforcement-mode",
		strings.NewReader(`{"enforcementMode":"AUDIT"}`),
		map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleSetDefaultEnforcementMode(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSetDefaultEnforcementMode_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/account-types/default-enforcement-mode",
		strings.NewReader(`{"enforcementMode":"STRICT"}`),
		map[string]string{
			"ledgerName": "",
		})

	srv.handleSetDefaultEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetDefaultEnforcementMode_MissingMode(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/account-types/default-enforcement-mode",
		strings.NewReader(`{}`),
		map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleSetDefaultEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetDefaultEnforcementMode_InvalidMode(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/account-types/default-enforcement-mode",
		strings.NewReader(`{"enforcementMode":"INVALID"}`),
		map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleSetDefaultEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetDefaultEnforcementMode_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/account-types/default-enforcement-mode",
		strings.NewReader(`not-json`),
		map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleSetDefaultEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
