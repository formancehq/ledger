package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

func TestClusterAddLearnerRejectsInvalidInstanceID(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		identity []byte
	}{
		{name: "missing"},
		{name: "short", identity: make([]byte, 15)},
		{name: "long", identity: make([]byte, 17)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Invalid requests are rejected before consulting local leadership or
			// forwarding to another node, so no node or membership dependency runs.
			impl := &ClusterServiceServerImpl{}
			response, err := impl.AddLearner(context.Background(), &clusterpb.AddLearnerRequest{
				NodeId: 2, RaftAddress: "node-2:7777", ServiceAddress: "node-2:8888", InstanceId: tt.identity,
			})
			require.Nil(t, response)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.ErrorContains(t, err, "instance_id must be 16 bytes")
		})
	}
}

func TestClusterAddLearnerRejectsMalformedAddresses(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name           string
		raftAddress    string
		serviceAddress string
		want           string
	}{
		{name: "raft", raftAddress: "not an address", serviceAddress: "node-2:8888", want: "raft_address \"not an address\" must be host:port"},
		{name: "service", raftAddress: "node-2:7777", serviceAddress: "also bad", want: "service_address \"also bad\" must be host:port"},
		{name: "port", raftAddress: "node-2:0", serviceAddress: "node-2:8888", want: "port must be a number between 1 and 65535"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Rejected before consulting leadership or forwarding, so the
			// nil node and membership dependencies are never reached.
			impl := &ClusterServiceServerImpl{}
			response, err := impl.AddLearner(context.Background(), &clusterpb.AddLearnerRequest{
				NodeId: 2, RaftAddress: tt.raftAddress, ServiceAddress: tt.serviceAddress, InstanceId: make([]byte, 16),
			})
			require.Nil(t, response)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.ErrorContains(t, err, tt.want)
		})
	}
}
