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

func TestHandleGetIndex_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.GetIndexRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
			capturedReq = req

			return &commonpb.Index{Ledger: req.GetLedger()}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, "ledger1", capturedReq.GetLedger())
	require.Equal(t, "color", capturedReq.GetId().GetMetadata().GetKey())
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, capturedReq.GetId().GetMetadata().GetTarget())
}

// TestHandleGetIndex_NamespacedMetadataKey pins the canonical-id unescape fix.
// A namespaced metadata key such as `formance.com/reviewed` contains a slash
// and a colon, which a client must percent-encode (`%2F`, `%3A`) so they don't
// split the chi route. chi routes on r.URL.RawPath and hands the still-escaped
// segment back via URLParam, so the handler must url.PathUnescape it before
// ParseCanonical — otherwise ParseCanonical sees the literal `%2F`/`%3A` and
// resolves the wrong key. This test injects the escaped form (what chi
// captures) and asserts the backend receives the decoded namespaced key.
func TestHandleGetIndex_NamespacedMetadataKey(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.GetIndexRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
			capturedReq = req

			return &commonpb.Index{Ledger: req.GetLedger()}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet,
		"/ledger1/indexes/metadata%3ATARGET_TYPE_ACCOUNT%3Aformance.com%2Freviewed", nil,
		map[string]string{
			"ledgerName": "ledger1",
			// The still-escaped segment, as chi captures it from RawPath.
			"canonicalId": "metadata%3ATARGET_TYPE_ACCOUNT%3Aformance.com%2Freviewed",
		})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, "formance.com/reviewed", capturedReq.GetId().GetMetadata().GetKey())
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, capturedReq.GetId().GetMetadata().GetTarget())
}

// TestHandleGetIndex_WireShape pins the public wire of the index detail route
// to the DTO rather than to the .proto descriptor (EN-1791). protojson derived
// the shape from the descriptor, which quotes 64-bit ints, wraps timestamps as
// {"data":<micros>} and drops meaningful zeros; the DTO owns all three.
func TestHandleGetIndex_WireShape(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
			return &commonpb.Index{
				Id:                     req.GetId(),
				Ledger:                 req.GetLedger(),
				BuildStatus:            commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
				ForwardEncodingVersion: 1,
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.True(t, strings.HasPrefix(strings.TrimSpace(body), `{"data":`),
		"response must be wrapped in the data envelope, got: %s", body)
	// The canonical id is the same string the route accepts as {canonicalId}, so
	// a response carries the address of its own detail route.
	require.Contains(t, body, `"canonicalId":"metadata:TARGET_TYPE_ACCOUNT:color"`)
	require.Contains(t, body, `"target":"TARGET_TYPE_ACCOUNT"`)
	require.Contains(t, body, `"buildStatus":"INDEX_BUILD_STATUS_READY"`)
	// Unquoted number: uint32 is not a 64-bit int, but the DTO renders every
	// numeric field the same way, so pinning one guards the convention.
	require.Contains(t, body, `"forwardEncodingVersion":1`)
	// No snake_case leak from the protoc-gen `json:` tags.
	require.NotContains(t, body, "build_status")
	// No untagged oneof wrapper leak: the generated IndexID_Metadata wrapper
	// carries only protobuf: tags, so a reflective marshal of it would emit
	// {"Kind":{...}}. newIndexIDDTO hand-dispatches instead.
	require.NotContains(t, body, `"Kind"`)
}

func TestHandleGetIndex_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.GetIndexRequest) (*commonpb.Index, error) {
			return nil, commonpb.NewNotFoundError("index not found")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetIndex_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP", nil, map[string]string{
		"ledgerName":  "",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetIndex_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/bogus", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "bogus",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
