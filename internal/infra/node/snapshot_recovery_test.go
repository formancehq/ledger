package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotRecoveryLifecycleRequiresPersistedIndex(t *testing.T) {
	n := &Node{snapshotRecovery: make(map[uint64]uint64)}
	n.markSnapshotRecoveryPending(2, 100)

	index, pending := n.SnapshotRecoveryPending(2)
	require.True(t, pending)
	require.Equal(t, uint64(100), index)

	n.ConfirmSnapshotRecovery(2, 99)
	_, pending = n.SnapshotRecoveryPending(2)
	require.True(t, pending, "a stale checkpoint confirmation must not close the episode")

	n.ConfirmSnapshotRecovery(2, 100)
	_, pending = n.SnapshotRecoveryPending(2)
	require.False(t, pending)
}
