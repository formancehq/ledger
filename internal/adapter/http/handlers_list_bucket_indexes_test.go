package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleListBucketIndexes_DefaultScopeAll(t *testing.T) {
	t.Parallel()

	var capturedScope servicepb.ListIndexesRequest_Scope

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListIndexes(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ListIndexesRequest) (cursor.Cursor[*commonpb.Index], error) {
			capturedScope = req.GetScope()

			return cursor.NewSliceCursor([]*commonpb.Index{
				{BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes", nil, nil)

	srv.handleListBucketIndexes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, servicepb.ListIndexesRequest_SCOPE_ALL, capturedScope)

	// Each list element must serialize in protobuf-JSON camelCase (buildStatus)
	// inside the {data:[...]} envelope, not the snake_case Go struct tag
	// (build_status). See writeProtoListOK.
	body := w.Body.String()
	require.Contains(t, body, `"buildStatus":`)
	require.NotContains(t, body, "build_status")
}

func TestHandleListBucketIndexes_ExplicitBucketScope(t *testing.T) {
	t.Parallel()

	var capturedScope servicepb.ListIndexesRequest_Scope

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListIndexes(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ListIndexesRequest) (cursor.Cursor[*commonpb.Index], error) {
			capturedScope = req.GetScope()

			return cursor.NewSliceCursor[*commonpb.Index](nil), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes?scope=bucket", nil, nil)

	srv.handleListBucketIndexes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, servicepb.ListIndexesRequest_SCOPE_BUCKET, capturedScope)
}

func TestHandleListBucketIndexes_LedgerScopeRejected(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes?scope=ledger", nil, nil)

	srv.handleListBucketIndexes(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "/v3/{ledgerName}/indexes")
}

func TestHandleListBucketIndexes_UnknownScope(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes?scope=bogus", nil, nil)

	srv.handleListBucketIndexes(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
