package http

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestHandleAddLearner(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AddLearner(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *clusterpb.AddLearnerRequest) (*clusterpb.AddLearnerResponse, error) {
		require.Equal(t, uint64(18446744073709551615), req.GetNodeId())
		require.Equal(t, "ledger-3.raft:7000", req.GetRaftAddress())
		require.Equal(t, "ledger-3.svc:8080", req.GetServiceAddress())
		require.Equal(t, []byte{0x0f, 0x8e, 0x1d, 0x2c, 0x3b, 0x4a, 0x45, 0x96, 0x87, 0x78, 0x69, 0x5a, 0x4b, 0x3c, 0x2d, 0x1e}, req.GetInstanceId())
		require.Equal(t, []uint32{1, 4}, req.GetCapabilities())

		return &clusterpb.AddLearnerResponse{}, nil
	})
	w := serveCluster(t, backend, http.MethodPost, "/v3/_/cluster/learners", `{
		"nodeId": "18446744073709551615",
		"raftAddress": "ledger-3.raft:7000",
		"serviceAddress": "ledger-3.svc:8080",
		"instanceId": "0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e",
		"capabilities": [1, 4]
	}`)
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	require.Empty(t, w.Body.String())
}

func TestHandleAddLearner_InvalidInput(t *testing.T) {
	t.Parallel()
	const valid = `"nodeId":2,"raftAddress":"r:1","serviceAddress":"s:1"`
	for name, body := range map[string]string{
		"missing node":         `{"raftAddress":"r:1","serviceAddress":"s:1","instanceId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e"}`,
		"zero node":            `{"nodeId":0,"raftAddress":"r:1","serviceAddress":"s:1","instanceId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e"}`,
		"missing raft address": `{"nodeId":2,"serviceAddress":"s:1","instanceId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e"}`,
		"missing service addr": `{"nodeId":2,"raftAddress":"r:1","instanceId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e"}`,
		"missing instance":     `{` + valid + `}`,
		"short instance":       `{` + valid + `,"instanceId":"0f8e1d2c"}`,
		"braced instance":      `{` + valid + `,"instanceId":"{0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e}"}`,
		"urn instance":         `{` + valid + `,"instanceId":"urn:uuid:0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e"}`,
		"unsorted capability":  `{` + valid + `,"instanceId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e","capabilities":[4,1]}`,
		"zero capability":      `{` + valid + `,"instanceId":"0f8e1d2c-3b4a-4596-8778-695a4b3c2d1e","capabilities":[0]}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := serveCluster(t, NewMockBackend(gomock.NewController(t)), http.MethodPost, "/v3/_/cluster/learners", body)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Equal(t, "INVALID_REQUEST", decodeErrorResponse(t, w).ErrorCode)
		})
	}
}
