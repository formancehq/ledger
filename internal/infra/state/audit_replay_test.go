package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAuditReplayerUsesRecoveredQueryCheckpointCounter(t *testing.T) {
	t.Parallel()

	replayer, err := NewAuditReplayer(logging.Testing(), "test-cluster")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replayer.Close()) })

	logs, err := replayer.Replay(&commonpb.Timestamp{Data: 1}, []*raftcmdpb.Order{{
		Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
			},
		}},
	}})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].GetPayload().GetCreatedQueryCheckpoint().GetCheckpointId())
}
