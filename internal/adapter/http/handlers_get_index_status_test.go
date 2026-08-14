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

	// camelCase in the {data:...} envelope, and uint64 as an unquoted NUMBER.
	// protojson quoted 64-bit ints ("42"); the DTO emits 42, matching
	// Chapter.MarshalJSON and the rest of the API (EN-1791).
	body := w.Body.String()
	require.Contains(t, body, `"lastIndexedSequence":42`)
	require.NotContains(t, body, `"lastIndexedSequence":"42"`)
	require.NotContains(t, body, "last_indexed_sequence")
	// lag 0 means fully caught up: it must be present, not omitted.
	require.Contains(t, body, `"lag":0`)
	// Empty collections are [], never null.
	require.Contains(t, body, `"indexes":[]`)
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
