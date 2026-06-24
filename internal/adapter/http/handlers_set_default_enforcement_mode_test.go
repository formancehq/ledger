package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleSetDefaultEnforcementMode_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
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

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/account-types/default-enforcement-mode",
		strings.NewReader(`not-json`),
		map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleSetDefaultEnforcementMode(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
