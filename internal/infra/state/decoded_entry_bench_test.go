package state

import (
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// BenchmarkDecodeEntriesProposal measures the actual decode and release path
// used by Raft apply, including the native batch envelope and historical wire.
func BenchmarkDecodeEntriesProposal(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		for _, native := range []bool{false, true} {
			name := "legacy"
			if native {
				name = "batch"
			}
			b.Run(fmt.Sprintf("%s/orders=%d", name, count), func(b *testing.B) {
				orders := make([]*raftcmdpb.Order, count)
				for i := range orders {
					orders[i] = &raftcmdpb.Order{}
				}
				updates := []*raftcmdpb.TechnicalUpdate{{}}
				proposal := &raftcmdpb.Proposal{}
				if native {
					proposal.Payload = &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
						Orders: orders, TechnicalUpdates: updates,
					}}
				} else {
					proposal.Orders = orders
					proposal.TechnicalUpdates = updates
				}
				data, err := proposal.MarshalVT()
				if err != nil {
					b.Fatal(err)
				}
				entries := []*raftpb.Entry{{Type: new(raftpb.EntryNormal), Data: data}}

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					decoded, err := DecodeEntries(entries)
					if err != nil {
						b.Fatal(err)
					}
					ReleaseDecodedEntries(decoded)
				}
			})
		}
	}
}
