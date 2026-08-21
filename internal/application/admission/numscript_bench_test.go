package admission

import (
	"context"
	"strings"
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
	benchmarkResolveScriptsWorldToBankBulk50(b, false)
}

// BenchmarkResolveScriptsWorldToBankUniqueBulk50 measures the no-hit path:
// every script is semantically identical and variable-free, but distinct text
// prevents request-local discovery reuse.
func BenchmarkResolveScriptsWorldToBankUniqueBulk50(b *testing.B) {
	benchmarkResolveScriptsWorldToBankBulk50(b, true)
}

func benchmarkResolveScriptsWorldToBankBulk50(b *testing.B, uniqueScripts bool) {
	store := createTestStore(b)
	admission, _ := createTestAdmission(b, store)

	orders := make([]*raftcmdpb.Order, 50)
	for i := range orders {
		script := worldToBankScript
		if uniqueScripts {
			// Trailing newlines preserve semantics while producing distinct memo
			// and parse-cache keys for every order.
			script += strings.Repeat("\n", i)
		}
		orders[i] = scriptOrder(testLedgerName, script)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// Admission receives fresh orders in production. The Numscript phase
		// stamps OrderTechnical, so reset it outside the timed region to ensure
		// every iteration measures the same allocation work.
		b.StopTimer()
		for _, order := range orders {
			order.Technical = nil
		}
		b.StartTimer()

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
