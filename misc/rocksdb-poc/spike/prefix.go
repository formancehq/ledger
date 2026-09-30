// Package spike holds the RocksDB POC experiments for the two Pebble
// features with no obvious one-to-one RocksDB equivalent:
//
//   - the read/usage store Comparer.Split (ledger-scoped bloom prefix), mapped
//     to a prefix extractor;
//   - the checker replay-store Merger, mapped to a MergeOperator.
//
// See docs/drafts/rocksdb-poc.md, step 2.
package spike

import "github.com/linxGnu/grocksdb"

// PrefixInternal mirrors the read store singleton-key prefix byte: those keys
// carry no ledger name and Pebble treats the whole key as the bloom prefix.
const PrefixInternal = 0x00

// LedgerNameFixedSize mirrors dal.LedgerNameFixedSize.
const LedgerNameFixedSize = 64

// LedgerScopedPrefixLen is the Pebble Split point for ledger-scoped keys:
// [prefix_byte][ledger_name padded 64B][...].
const LedgerScopedPrefixLen = 1 + LedgerNameFixedSize

// NewFixedLedgerPrefixExtractor returns RocksDB's native fixed-length prefix
// extractor sized to the ledger-scoped prefix. It runs entirely in C++ (no
// cgo callback per key). Semantics versus the Pebble Split:
//
//   - ledger-scoped keys (len >= 65): identical prefix;
//   - keys shorter than 65 bytes: out of domain, so no prefix bloom entry —
//     whole-key filtering still covers point lookups on them;
//   - internal singleton keys >= 65 bytes: prefix is their first 65 bytes
//     instead of the whole key. Harmless: such keys are read by point
//     lookup, never by prefix scan.
func NewFixedLedgerPrefixExtractor() grocksdb.SliceTransform {
	return grocksdb.NewFixedPrefixTransform(LedgerScopedPrefixLen)
}

// SplitPrefixExtractor is a byte-for-byte port of readStoreSplit as a Go
// SliceTransform. Every call crosses the cgo boundary; it exists to measure
// that cost against the native fixed extractor, not as the recommended
// mapping.
type SplitPrefixExtractor struct{}

func (SplitPrefixExtractor) Transform(src []byte) []byte { return src[:splitLen(src)] }

func (SplitPrefixExtractor) InDomain([]byte) bool { return true }

func (SplitPrefixExtractor) InRange([]byte) bool { return true }

func (SplitPrefixExtractor) Name() string { return "formance.readstore.split" }

func (SplitPrefixExtractor) Destroy() {}

func splitLen(key []byte) int {
	if len(key) <= 1 {
		return len(key)
	}
	if key[0] == PrefixInternal {
		return len(key)
	}
	if len(key) >= LedgerScopedPrefixLen {
		return LedgerScopedPrefixLen
	}

	return len(key)
}

// LedgerKey builds a ledger-scoped key: [prefix][name padded to 64B][suffix].
func LedgerKey(prefix byte, ledger string, suffix []byte) []byte {
	key := make([]byte, 0, LedgerScopedPrefixLen+len(suffix))
	key = append(key, prefix)
	key = append(key, ledger...)
	for i := len(ledger); i < LedgerNameFixedSize; i++ {
		key = append(key, ' ')
	}

	return append(key, suffix...)
}
