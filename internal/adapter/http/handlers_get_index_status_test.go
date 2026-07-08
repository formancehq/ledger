package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleGetIndexStatus_Success(t *testing.T) {
	t.Parallel()

	var capturedLedger string

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
			capturedLedger = req.GetLedger()

			return &servicepb.GetIndexStatusResponse{LastIndexedSequence: 42}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/status?ledger=ledger1", nil, nil)

	srv.handleGetIndexStatus(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "ledger1", capturedLedger)
}

func TestHandleGetIndexStatus_NoLedgerFilter(t *testing.T) {
	t.Parallel()

	var capturedLedger string

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
			capturedLedger = req.GetLedger()

			return &servicepb.GetIndexStatusResponse{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/status", nil, nil)

	srv.handleGetIndexStatus(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "", capturedLedger)
}

func TestHandleGetIndexStatus_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
			return nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/status", nil, nil)

	srv.handleGetIndexStatus(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
