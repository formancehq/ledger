package state

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Measures the real serialized apply/commit boundary at increasing proposal
// sizes. Backup-bearing preparation has a separate active-job cardinality
// benchmark; ordinary sink proposals must not clone any active-job state.
func BenchmarkTechnicalAtomicityApply(b *testing.B) {
	for _, marked := range []bool{false, true} {
		for _, count := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("marked=%t/updates=%d", marked, count), func(b *testing.B) {
				machine, store, _ := newTestMachine(b)
				proposal := makeProposal(1)
				proposal.AtomicTechnicalUpdates = marked
				for i := range count {
					proposal.TechnicalUpdates = append(proposal.TechnicalUpdates, sinkCursorUpdate(fmt.Sprintf("sink-%d", i), 42))
				}
				data, err := proposal.MarshalVT()
				require.NoError(b, err)
				index := uint64(0)
				entry := &raftpb.Entry{Index: &index, Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index++
					result, err := machine.ApplyEntries(b.Context(), store, entry)
					require.NoError(b, err)
					require.NoError(b, result.Results[0].Error)
				}
			})
		}
	}
}
