package admission

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const worldToBankScript = `send [USD/2 100] (
  source = @world
  destination = @bank
)`

// BenchmarkResolveScriptsWorldToBankBulk50 models the Numscript phase of the
// perf-world-to-bank k6 workload: one atomic bulk containing 50 identical,
// fully-static transactions.
func BenchmarkResolveScriptsWorldToBankBulk50(b *testing.B) {
	store := createTestStore(b)
	admission, _ := createTestAdmission(b, store)

	orders := make([]*raftcmdpb.Order, 50)
	for i := range orders {
		orders[i] = scriptOrder(testLedgerName, worldToBankScript)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		overlay := newBulkOverlay()
		needs, perOrder, err := admission.extractPreloadNeeds(context.Background(), orders, overlay)
		if err != nil {
			b.Fatal(err)
		}

		if err := admission.resolveScriptsAndEnrichNeeds(
			context.Background(), orders, overlay, needs, perOrder, false,
		); err != nil {
			b.Fatal(err)
		}
	}
}
