package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestDiscoveryReturnsServerInfo(t *testing.T) {
	t.Parallel()

	impl := &BucketServiceServerImpl{
		info: version.Info{
			Version:   "v3.1.0",
			Commit:    "abc1234",
			BuildDate: "2026-06-19T00:00:00Z",
			GoVersion: "go1.24",
		},
	}

	resp, err := impl.Discovery(context.Background(), &servicepb.DiscoveryRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.GetServerInfo())
	require.Equal(t, "v3.1.0", resp.GetServerInfo().GetVersion())
	require.Equal(t, "abc1234", resp.GetServerInfo().GetCommit())
	require.Equal(t, "2026-06-19T00:00:00Z", resp.GetServerInfo().GetBuildDate())
	require.Equal(t, "go1.24", resp.GetServerInfo().GetGoVersion())
}

func TestGetClusterStateMapsPeerVersion(t *testing.T) {
	t.Parallel()

	// A reachable peer reporting its version is surfaced verbatim.
	require.Equal(t, "v3.0.9", mapNodeVersion(&clusterpb.ClusterState{NodeVersion: "v3.0.9"}))
	// A peer on a binary predating node_version (empty) must NOT be masked by
	// the local/leader version — the skew has to remain visible.
	require.Equal(t, "", mapNodeVersion(&clusterpb.ClusterState{NodeVersion: ""}))
	// An unreachable peer (nil state) likewise must NOT be masked.
	require.Equal(t, "", mapNodeVersion(nil))
}
