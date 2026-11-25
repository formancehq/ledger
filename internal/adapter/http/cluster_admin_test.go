package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	internalgrpc "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func serveCluster(t *testing.T, backend Backend, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil).ServeHTTP(w, r)

	return w
}

func decodeErrorResponse(t *testing.T, w *httptest.ResponseRecorder) ErrorResponse {
	t.Helper()
	var resp ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp), w.Body.String())

	return resp
}

func TestClusterNodeID_Decoding(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input string
		want  uint64
		err   bool
	}{
		{`1`, 1, false},
		{`18446744073709551615`, 18446744073709551615, false},
		{`"18446744073709551615"`, 18446744073709551615, false},
		{`0`, 0, true},
		{`"0"`, 0, true},
		{`-1`, 0, true},
		{`1.5`, 0, true},
		{`1e3`, 0, true},
		{`18446744073709551616`, 0, true},
		{`"0x10"`, 0, true},
		{`" 1"`, 0, true},
		{`true`, 0, true},
		{`null`, 0, true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			var id clusterNodeID
			err := id.UnmarshalJSON([]byte(tc.input))
			if tc.err {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, uint64(id))
		})
	}
}

func TestClusterAdmin_RejectsMalformedBodies(t *testing.T) {
	t.Parallel()
	for _, target := range []string{
		"/v3/_/cluster/leadership/transfer",
		"/v3/_/cluster/learners",
		"/v3/_/cluster/drain",
		"/v3/_/cluster/drain/end",
	} {
		for name, body := range map[string]string{
			"empty":         "",
			"not an object": `[]`,
			"unknown field": `{"unknown":1}`,
			"trailing data": `{} {}`,
			"duplicate key": `{"epoch":"a","epoch":"b"}`,
		} {
			t.Run(target+"/"+name, func(t *testing.T) {
				t.Parallel()
				// The mock has no expectation: reaching the backend fails the test.
				w := serveCluster(t, NewMockBackend(gomock.NewController(t)), http.MethodPost, target, body)
				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				require.Equal(t, "INVALID_REQUEST", decodeErrorResponse(t, w).ErrorCode)
			})
		}
	}
}

func TestClusterAdmin_RejectsInvalidPathNodeID(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"0", "-1", "abc", "18446744073709551616", "%2B1"} {
		for _, route := range []struct{ method, target string }{
			{http.MethodPost, "/v3/_/cluster/learners/" + id + "/promote"},
			{http.MethodDelete, "/v3/_/cluster/nodes/" + id},
		} {
			t.Run(route.method+" "+route.target, func(t *testing.T) {
				t.Parallel()
				w := serveCluster(t, NewMockBackend(gomock.NewController(t)), route.method, route.target, "")
				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				require.Equal(t, "INVALID_REQUEST", decodeErrorResponse(t, w).ErrorCode)
			})
		}
	}
}

// TestClusterAdmin_ErrorMapping pins the HTTP rendering of each failure the
// ClusterService can return, both raised locally on the leader and decoded
// from a forwarded leader status.
func TestClusterAdmin_ErrorMapping(t *testing.T) {
	t.Parallel()
	removalCommitted := &node.RemoveNodeCommittedError{NodeID: 3, CommittedIndex: 42}
	forwardedRemoval, ok := internalgrpc.KnownErrorStatus(removalCommitted)
	require.True(t, ok)

	for _, tc := range []struct {
		name       string
		err        error
		status     int
		code       string
		message    string
		retryAfter bool
	}{
		{"removal committed", removalCommitted, http.StatusServiceUnavailable, "RAFT_NODE_REMOVAL_COMMITTED", removalCommitted.Error(), true},
		{"forwarded removal committed", forwardedRemoval.Err(), http.StatusServiceUnavailable, "RAFT_NODE_REMOVAL_COMMITTED", removalCommitted.Error(), true},
		{"node not in cluster", fmt.Errorf("removing node: %w", node.ErrNodeNotInCluster), http.StatusNotFound, "RAFT_NODE_NOT_IN_CLUSTER", "removing node: " + node.ErrNodeNotInCluster.Error(), false},
		{"node already in cluster", fmt.Errorf("adding learner: %w", node.ErrNodeAlreadyInCluster), http.StatusConflict, "ALREADY_EXISTS", "adding learner: " + node.ErrNodeAlreadyInCluster.Error(), false},
		{"unknown transferee", fmt.Errorf("leadership transfer failed: %w", node.ErrUnknownTransferee), http.StatusConflict, "FAILED_PRECONDITION", "leadership transfer failed: " + node.ErrUnknownTransferee.Error(), false},
		{"drain epoch conflict", status.Error(codes.FailedPrecondition, "cluster is already draining under another epoch"), http.StatusConflict, "FAILED_PRECONDITION", "cluster is already draining under another epoch", false},
		{"invalid epoch", status.Error(codes.InvalidArgument, "drain epoch must contain 1 to 128 bytes"), http.StatusBadRequest, "INVALID_REQUEST", "drain epoch must contain 1 to 128 bytes", false},
		{"deadline exceeded", fmt.Errorf("adding learner: %w", context.DeadlineExceeded), http.StatusServiceUnavailable, "UNAVAILABLE", "unavailable", true},
		{"cancelled", context.Canceled, http.StatusServiceUnavailable, "UNAVAILABLE", "unavailable", true},
		{"forwarded unavailable", status.Error(codes.Unavailable, "member 2 drain preflight: dial"), http.StatusServiceUnavailable, "UNAVAILABLE", "unavailable", true},
		{"no leader", commonpb.ErrNoLeader, http.StatusServiceUnavailable, "NO_LEADER", commonpb.ErrNoLeader.Error(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().RemoveNode(gomock.Any(), gomock.Any()).Return(nil, tc.err)
			w := serveCluster(t, backend, http.MethodDelete, "/v3/_/cluster/nodes/3", "")
			require.Equal(t, tc.status, w.Code, w.Body.String())
			resp := decodeErrorResponse(t, w)
			require.Equal(t, tc.code, resp.ErrorCode)
			require.Equal(t, tc.message, resp.ErrorMessage)
			if tc.retryAfter {
				require.Equal(t, "1", w.Header().Get("Retry-After"))
			} else {
				require.Empty(t, w.Header().Get("Retry-After"))
			}
		})
	}
}

func TestClusterAdmin_SanitizesUnclassifiedFailures(t *testing.T) {
	t.Parallel()
	for name, err := range map[string]error{
		"local":     errors.New("compaction failed: /data/pebble/000123.sst: corrupted"),
		"forwarded": status.Error(codes.Unknown, "unknown server error (correlation ID: abc)"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().CompactPrimary(gomock.Any(), gomock.Any()).Return(nil, err)
			w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/compactions/primary", "")
			require.Equal(t, http.StatusInternalServerError, w.Code)
			resp := decodeErrorResponse(t, w)
			require.Equal(t, "INTERNAL_ERROR", resp.ErrorCode)
			require.NotContains(t, resp.ErrorMessage, "pebble")
			require.NotContains(t, resp.ErrorMessage, "abc")
		})
	}
}
