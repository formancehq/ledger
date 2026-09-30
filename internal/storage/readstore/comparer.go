package readstore

import "bytes"

// readStoreComparerName is persisted in RocksDB OPTIONS and must match the
// bytewise comparator used by the offline migration tool.
const readStoreComparerName = "formance.readstore.v2"

// readStoreSplit identifies the ledger-scoped prefix used for bounded scans.
// Prefix bloom optimization can be added once the RocksDB extractor lifetime
// is qualified; lexicographic ordering remains unchanged.
func readStoreSplit(key []byte) int {
	if len(key) <= 1 || key[0] == PrefixInternal {
		return len(key)
	}
	if len(key) >= ledgerScopedPrefixLen {
		return ledgerScopedPrefixLen
	}

	return len(key)
}

// ReadStoreComparer preserves the old test contract for key ordering and
// prefix rules while RocksDB persists the named bytewise comparator above.
var ReadStoreComparer = struct {
	Compare            func([]byte, []byte) int
	Split              func([]byte) int
	ImmediateSuccessor func([]byte, []byte) []byte
}{
	Compare: bytes.Compare,
	Split:   readStoreSplit,
	ImmediateSuccessor: func(dst, prefix []byte) []byte {
		dst = append(dst[:0], prefix...)
		if len(dst) == ledgerScopedPrefixLen {
			dst[len(dst)-1]++

			return dst
		}

		return append(dst, 0)
	},
}
