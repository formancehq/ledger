package readstore

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// addressBenchPage mirrors the diagnostic read: a single page of results over
// a fully materialized union. Materialization dominates the cost; how many
// entries are consumed afterwards does not change the sort-once fix's effect.
const addressBenchPage = 100

// runAddressTxBenchmark writes the account→tx rows, then repeatedly
// re-materializes the union from scratch (a fresh AddressTxIterator per
// iteration) and consumes the first addressBenchPage entries. scanned-rows and
// unique-ids are reported so duplicate-heavy workloads record the dedup
// pressure alongside the materialized output size.
func runAddressTxBenchmark(b *testing.B, txsByAccount map[string][]uint64, addrs ...string) {
	b.Helper()

	s, err := New(b.TempDir(), logging.NopZap(), DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })

	kb := dal.NewKeyBuilder()
	scanned := 0
	unique := make(map[uint64]struct{})
	for account, txs := range txsByAccount {
		scanned += len(txs)
		for _, id := range txs {
			unique[id] = struct{}{}
			if err := s.DB().Set(AccountTxKey(kb, PrefixAccountTx, "l", account, id), nil, pebble.NoSync); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportMetric(float64(scanned), "scanned-rows")
	b.ReportMetric(float64(len(unique)), "unique-ids")

	b.ResetTimer()
	for range b.N {
		it := NewAddressTxIterator(s.DB(), dal.NewKeyBuilder(), "l", newAliasingIter(addrs...), PrefixAccountTx)
		for n := 0; n < addressBenchPage && it.Next(); n++ {
			_ = it.Current()
		}
		if err := it.Err(); err != nil {
			it.Close()
			b.Fatal(err)
		}
		it.Close()
	}
}

// BenchmarkAddressTxIterator_InterleavedLargeHistory is the diagnostic case
// from the ticket: two account histories whose IDs interleave (evens/odds), so
// the pre-fix per-ID insertSorted shifted an ever-growing sorted slice for
// every unseen ID — O(U²) movement. Append-then-sort-once removes that.
func BenchmarkAddressTxIterator_InterleavedLargeHistory(b *testing.B) {
	const total = 50_000

	txs := map[string][]uint64{
		"acc:even": make([]uint64, 0, total/2),
		"acc:odd":  make([]uint64, 0, total/2),
	}
	for id := range uint64(total) {
		if id%2 == 0 {
			txs["acc:even"] = append(txs["acc:even"], id)
		} else {
			txs["acc:odd"] = append(txs["acc:odd"], id)
		}
	}

	runAddressTxBenchmark(b, txs, "acc:even", "acc:odd")
}

// BenchmarkAddressTxIterator_SortedSingleAccount is the already-sorted
// control: IDs arrive in ascending order, so insertSorted never shifted and
// the pre-fix code was already near-linear. This workload reports any
// regression from the unconditional final sort.
func BenchmarkAddressTxIterator_SortedSingleAccount(b *testing.B) {
	const total = 50_000

	txs := map[string][]uint64{"acc:0": make([]uint64, 0, total)}
	for id := range uint64(total) {
		txs["acc:0"] = append(txs["acc:0"], id)
	}

	runAddressTxBenchmark(b, txs, "acc:0")
}

// BenchmarkAddressTxIterator_DuplicateHeavy stresses the dedup path: many
// matching accounts reference the same small set of transaction IDs (the same
// shape a transaction touching several matching accounts, or an account in
// multiple address roles, produces against the any-role index). scanned-rows
// (accounts × IDs) and unique-ids are reported via metrics.
func BenchmarkAddressTxIterator_DuplicateHeavy(b *testing.B) {
	const (
		accounts      = 200
		idsPerAccount = 250
	)

	txs := make(map[string][]uint64, accounts)
	addrs := make([]string, 0, accounts)
	for a := range accounts {
		addr := fmt.Sprintf("acc:%d", a)
		addrs = append(addrs, addr)

		ids := make([]uint64, 0, idsPerAccount)
		for id := range uint64(idsPerAccount) {
			ids = append(ids, id)
		}
		txs[addr] = ids
	}

	runAddressTxBenchmark(b, txs, addrs...)
}
