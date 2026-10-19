package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

	// The body must serialize the protobuf message in protobuf-JSON camelCase
	// (lastIndexedSequence), wrapped in the {data:...} envelope — NOT the
	// snake_case Go struct tags (last_indexed_sequence) that a plain sonic
	// marshal would leak. See writeProtoOK.
	body := w.Body.String()
	require.Contains(t, body, `"lastIndexedSequence":"42"`)
	require.NotContains(t, body, "last_indexed_sequence")
	require.True(t, strings.HasPrefix(strings.TrimSpace(body), `{"data":`), "response must be wrapped in the data envelope, got: %s", body)
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
