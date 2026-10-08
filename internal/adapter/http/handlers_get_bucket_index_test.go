package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	protoerr "github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetBucketIndex_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *ledgerpb.GetIndexRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *ledgerpb.GetIndexRequest) (*ledgerpb.Index, error) {
			capturedReq = req

			return &ledgerpb.Index{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/log_builtin:LOG_BUILTIN_INDEX_DATE", nil, map[string]string{
		"canonicalId": "log_builtin:LOG_BUILTIN_INDEX_DATE",
	})

	srv.handleGetBucketIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	// Bucket-scoped call: no ledger in the request.
	require.Equal(t, "", capturedReq.GetLedger())
	require.NotNil(t, capturedReq.GetId().GetLogBuiltin())
}

func TestHandleGetBucketIndex_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *ledgerpb.GetIndexRequest) (*ledgerpb.Index, error) {
			return nil, protoerr.NewNotFoundError("index not found")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/log_builtin:LOG_BUILTIN_INDEX_DATE", nil, map[string]string{
		"canonicalId": "log_builtin:LOG_BUILTIN_INDEX_DATE",
	})

	srv.handleGetBucketIndex(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetBucketIndex_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/bogus", nil, map[string]string{
		"canonicalId": "bogus",
	})

	srv.handleGetBucketIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
