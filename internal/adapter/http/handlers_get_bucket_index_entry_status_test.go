package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleGetBucketIndexEntryStatus_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.GetIndexEntryStatusRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexEntryStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
			capturedReq = req

			return &servicepb.IndexEntry{CurrentVersion: 1}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/log_builtin:LOG_BUILTIN_INDEX_DATE/status", nil, map[string]string{
		"canonicalId": "log_builtin:LOG_BUILTIN_INDEX_DATE",
	})

	srv.handleGetBucketIndexEntryStatus(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, "", capturedReq.GetLedger())
}

func TestHandleGetBucketIndexEntryStatus_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexEntryStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
			return nil, commonpb.NewNotFoundError("not found")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/log_builtin:LOG_BUILTIN_INDEX_DATE/status", nil, map[string]string{
		"canonicalId": "log_builtin:LOG_BUILTIN_INDEX_DATE",
	})

	srv.handleGetBucketIndexEntryStatus(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetBucketIndexEntryStatus_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/bogus/status", nil, map[string]string{
		"canonicalId": "bogus",
	})

	srv.handleGetBucketIndexEntryStatus(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
