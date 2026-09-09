package readstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// addressTxPageSize is the page a paginated address query reads. The union is
// materialized in full regardless, so this is the shape the diagnostic
// measurement in EN-1965 used (a page of 100 over a large union).
const addressTxPageSize = 100

// addressTxUnionBench describes an account→tx layout for
// benchmarkAddressTxUnion.
type addressTxUnionBench struct {
	// uniqueIDs is the number of distinct transaction IDs in the union.
	uniqueIDs int
	// accounts is the number of matched account addresses.
	accounts int
	// duplicated writes every unique ID under every account, so the scan
	// reads accounts*uniqueIDs rows for uniqueIDs union entries.
	duplicated bool
	// prefix is the account→tx bucket, as picked by addressRolePrefix.
	prefix byte
}

// benchmarkAddressTxUnion measures materializing the union and reading one
// page. The store is written once; each iteration builds a fresh iterator so
// the one-time materialization is what gets measured.
func benchmarkAddressTxUnion(b *testing.B, cfg addressTxUnionBench) {
	b.Helper()

	s := newTestStore(b)
	kb := dal.NewKeyBuilder()

	addrs := make([]string, 0, cfg.accounts)
	for a := range cfg.accounts {
		addrs = append(addrs, "acc:"+string(rune('a'+a%26))+string(rune('0'+a/26)))
	}

	batch := s.DB().NewBatch()
	scannedRows := 0

	for id := range cfg.uniqueIDs {
		// Interleaved: consecutive IDs belong to different accounts, so every
		// account contributes IDs that sort before IDs another account already
		// appended. A single account degenerates to an ascending scan.
		owners := addrs[id%len(addrs) : id%len(addrs)+1]
		if cfg.duplicated {
			owners = addrs
		}

		for _, account := range owners {
			require.NoError(b, batch.Set(AccountTxKey(kb, cfg.prefix, "l", account, uint64(id+1)), nil, nil))
			scannedRows++
		}
	}

	require.NoError(b, batch.Commit(pebble.NoSync))
	require.NoError(b, batch.Close())

	b.ReportAllocs()

	for b.Loop() {
		it := NewAddressTxIterator(s.DB(), kb, "l", newAliasingIter(addrs...), cfg.prefix)

		read := 0
		for read < addressTxPageSize && it.Next() {
			read++
		}

		if err := it.Err(); err != nil {
			b.Fatal(err)
		}

		it.Close()
	}

	// Reported after the loop: b.Loop resets the timer on its first call,
	// which also clears metrics reported before it.
	b.ReportMetric(float64(cfg.uniqueIDs), "uniqueIDs")
	b.ReportMetric(float64(scannedRows), "scannedRows")
}

// Interleaved histories: the layout whose per-insertion tail shifts were
// quadratic in the size of the union.
func BenchmarkAddressTxUnion_Interleaved_1k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{uniqueIDs: 1_000, accounts: 4, prefix: PrefixAccountTx})
}

func BenchmarkAddressTxUnion_Interleaved_10k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{uniqueIDs: 10_000, accounts: 4, prefix: PrefixAccountTx})
}

func BenchmarkAddressTxUnion_Interleaved_50k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{uniqueIDs: 50_000, accounts: 4, prefix: PrefixAccountTx})
}

// Sorted single account: the append order is already ascending, so this is
// where sorting the completed slice can only lose. It exists to make any
// small-history regression visible rather than implicit.
func BenchmarkAddressTxUnion_SortedSingleAccount_1k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{uniqueIDs: 1_000, accounts: 1, prefix: PrefixAccountTx})
}

func BenchmarkAddressTxUnion_SortedSingleAccount_10k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{uniqueIDs: 10_000, accounts: 1, prefix: PrefixAccountTx})
}

func BenchmarkAddressTxUnion_SortedSingleAccount_50k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{uniqueIDs: 50_000, accounts: 1, prefix: PrefixAccountTx})
}

// Duplicate heavy: every account holds every ID, so scannedRows is four times
// uniqueIDs and the dedup map absorbs the difference before the sort.
func BenchmarkAddressTxUnion_DuplicateHeavy_1k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{
		uniqueIDs: 1_000, accounts: 4, duplicated: true, prefix: PrefixAccountTx,
	})
}

func BenchmarkAddressTxUnion_DuplicateHeavy_10k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{
		uniqueIDs: 10_000, accounts: 4, duplicated: true, prefix: PrefixAccountTx,
	})
}

func BenchmarkAddressTxUnion_DuplicateHeavy_50k(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{
		uniqueIDs: 50_000, accounts: 4, duplicated: true, prefix: PrefixAccountTx,
	})
}

// The source and destination buckets carry the same layout, so a regression
// confined to one address role stays visible.
func BenchmarkAddressTxUnion_Interleaved_10k_Source(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{uniqueIDs: 10_000, accounts: 4, prefix: PrefixSourceAccountTx})
}

func BenchmarkAddressTxUnion_Interleaved_10k_Destination(b *testing.B) {
	benchmarkAddressTxUnion(b, addressTxUnionBench{
		uniqueIDs: 10_000, accounts: 4, prefix: PrefixDestinationAccountTx,
	})
}
