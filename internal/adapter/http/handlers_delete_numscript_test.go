package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleDeleteNumscript_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/numscripts/my-script", nil, map[string]string{
		"ledgerName": "ledger1",
		"name":       "my-script",
	})

	srv.handleDeleteNumscript(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleDeleteNumscript_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/numscripts/my-script", nil, map[string]string{
		"ledgerName": "",
		"name":       "my-script",
	})

	srv.handleDeleteNumscript(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteNumscript_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/numscripts/", nil, map[string]string{
		"ledgerName": "ledger1",
		"name":       "",
	})

	srv.handleDeleteNumscript(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteNumscript_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrNumscriptNotFound{Name: "my-script"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/numscripts/my-script", nil, map[string]string{
		"ledgerName": "ledger1",
		"name":       "my-script",
	})

	srv.handleDeleteNumscript(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
