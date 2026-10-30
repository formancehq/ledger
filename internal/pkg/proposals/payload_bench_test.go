package proposals

import (
	"fmt"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Reading the envelope never walks or clones its orders, regardless of batch
// cardinality. The same borrowed objects retain admission's coverage targets.
func BenchmarkPayloadReaders(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("orders=%d", count), func(b *testing.B) {
			p := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: make([]*raftcmdpb.Order, count)}}}
			b.ReportAllocs()
			for b.Loop() {
				if len(BatchOrders(p)) != count || IntentCount(p) != count || len(TechnicalUpdates(p)) != 0 {
					b.Fatal("unexpected batch contents")
				}
			}
		})
	}
}
