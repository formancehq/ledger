package node

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The lookup partition is the pending history of the requested ordinal, not
// all pending removals in the cluster. This checks the cardinality boundary
// independently of noisy wall-clock performance measurements.
func TestPendingRemovalSetPartitionsEventsByNode(t *testing.T) {
	t.Parallel()
	var pending pendingRemovalSet
	for nodeID := uint64(1); nodeID <= 1024; nodeID++ {
		for index := uint64(1); index <= 3; index++ {
			identity := make([]byte, 16)
			binary.BigEndian.PutUint64(identity, nodeID)
			binary.BigEndian.PutUint64(identity[8:], index)
			_, loaded := pending.loadOrStore(nodeID, &pendingRemoval{instanceID: identity, committedIndex: index})
			require.False(t, loaded)
		}
	}
	require.Len(t, pending.byNode, 1024)
	require.Len(t, pending.byNode[2], 3, "lookup work must be scoped to this ordinal's outstanding events")
	for _, event := range pending.byNode[2] {
		require.True(t, pending.contains(2, event.instanceID))
		require.False(t, pending.contains(3, event.instanceID))
		require.True(t, pending.compareAndDelete(2, event))
	}
	require.NotContains(t, pending.byNode, uint64(2), "completed ordinals must not retain empty partitions")
	require.Len(t, pending.byNode, 1023, "cleanup must retain unrelated ordinals")
}

func TestPendingRemovalSetCleanupRequiresOriginalEvent(t *testing.T) {
	t.Parallel()
	var pending pendingRemovalSet
	old := &pendingRemoval{instanceID: []byte("old-instance-id-"), committedIndex: 1}
	pending.loadOrStore(2, old)
	require.True(t, pending.compareAndDelete(2, old))
	replacement := &pendingRemoval{instanceID: []byte("old-instance-id-"), committedIndex: 1}
	pending.loadOrStore(2, replacement)
	require.False(t, pending.compareAndDelete(2, old), "an obsolete pointer must not release a replacement event")
	require.True(t, pending.contains(2, replacement.instanceID))
	require.True(t, pending.compareAndDelete(2, replacement))
}

func BenchmarkPendingRemovalAdmission(b *testing.B) {
	for _, events := range []int{1, 64, 1024} {
		for _, otherNodes := range []int{0, 4096} {
			b.Run(fmt.Sprintf("events=%d/otherNodes=%d", events, otherNodes), func(b *testing.B) {
				var pending pendingRemovalSet
				for i := 1; i <= events; i++ {
					identity := make([]byte, 16)
					binary.BigEndian.PutUint64(identity, uint64(i))
					pending.loadOrStore(1, &pendingRemoval{instanceID: identity, committedIndex: uint64(i)})
				}
				for i := range otherNodes {
					pending.loadOrStore(uint64(i+2), &pendingRemoval{instanceID: make([]byte, 16), committedIndex: 1})
				}
				freshIdentity := []byte("fresh-instanceid")
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if pending.contains(1, freshIdentity) {
						b.Fatal("fresh identity must remain eligible")
					}
				}
			})
		}
	}
}
