package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"
)

// readStoreComparerName is persisted in the Pebble database. Changing it
// requires rebuilding the read index from the Raft log.
const readStoreComparerName = "formance.readstore.v1"

// ledgerIDSize is the fixed size of a uint32 big-endian ledger ID in keys.
const ledgerIDSize = 4

// ledgerScopedPrefixLen is the split point for ledger-scoped keys:
// 1 byte prefix + 4 bytes uint32 ledger ID.
const ledgerScopedPrefixLen = 1 + ledgerIDSize

// ReadStoreComparer is a Pebble comparer that splits keys at the
// [prefix_byte][ledger_id_BE_4B] boundary so that bloom filters are built on
// the ledger-scoped prefix rather than the full key.
//
// This enables SeekPrefixGE to check bloom filters during range scans,
// skipping entire SSTables that do not contain keys for the target ledger.
// The benefit is proportional to the number of distinct ledgers sharing the
// read index: with N ledgers, read amplification for per-ledger queries
// drops by ~N×.
//
// Key ordering is unchanged (lexicographic bytes.Compare).
var ReadStoreComparer = &pebble.Comparer{
	// Ordering: unchanged from default (lexicographic byte order).
	Compare:              bytes.Compare,
	Equal:                bytes.Equal,
	ComparePointSuffixes: bytes.Compare,
	CompareRangeSuffixes: bytes.Compare,

	AbbreviatedKey: func(key []byte) uint64 {
		if len(key) >= 8 {
			return binary.BigEndian.Uint64(key)
		}

		var v uint64
		for _, b := range key {
			v <<= 8
			v |= uint64(b)
		}

		return v << uint(8*(8-len(key)))
	},

	FormatKey: pebble.DefaultComparer.FormatKey,

	Separator: func(dst, a, b []byte) []byte {
		i := commonPrefixLen(a, b)
		dst = append(dst, a...)

		if i == len(a) || i == len(b) {
			return dst
		}

		if a[i] >= b[i] {
			return dst
		}

		// Attempt to shorten: pick the byte midway and truncate.
		n := len(dst) - len(a)
		if c := a[i] + 1; c < b[i] {
			dst[n+i] = c

			return dst[:n+i+1]
		}

		return dst
	},

	Successor: func(dst, a []byte) []byte {
		for i := range a {
			if a[i] != 0xff {
				dst = append(dst, a[:i+1]...)
				dst[len(dst)-1]++

				return dst
			}
		}

		return append(dst, a...)
	},

	// Split extracts the ledger-scoped prefix from a read store key.
	//
	// For keys following [prefix_byte][ledger_id_BE_4B][...], Split returns
	// ledgerScopedPrefixLen (= 5): 1 byte prefix + 4 bytes uint32 ledger ID.
	//
	// For singleton keys (PrefixInternal) that have no ledger ID, Split
	// returns len(key) — the entire key is the prefix (same as the default
	// comparer).
	Split: readStoreSplit,

	// ImmediateSuccessor returns the smallest prefix larger than the given prefix.
	//
	// For ledger-scoped prefixes (5 bytes: [prefix_byte][ledger_id_BE_4B]),
	// increment the uint32 ledger ID. All keys with the original ledger ID
	// sort between [prefix][id] and [prefix][id+1].
	//
	// For fallback prefixes (singleton keys), append 0x00.
	ImmediateSuccessor: func(dst, prefix []byte) []byte {
		dst = append(dst[:0], prefix...)
		if len(dst) == ledgerScopedPrefixLen {
			// Ledger-scoped prefix: increment the uint32 ledger ID.
			id := binary.BigEndian.Uint32(dst[1:])
			binary.BigEndian.PutUint32(dst[1:], id+1)

			return dst
		}

		// Fallback (singleton keys): append 0x00.
		return append(dst, 0x00)
	},

	Name: readStoreComparerName,
}

// readStoreSplit returns the split point for bloom filter prefix extraction.
func readStoreSplit(key []byte) int {
	if len(key) <= 1 {
		return len(key)
	}

	// Internal singleton keys (non-ledger-scoped) — full key is the prefix.
	if key[0] == PrefixInternal {
		return len(key)
	}

	// Ledger-scoped keys: [prefix_byte][ledger_id_BE_4B][...].
	// The prefix is the first 5 bytes (1 + 4).
	if len(key) >= ledgerScopedPrefixLen {
		return ledgerScopedPrefixLen
	}

	// Key shorter than expected — treat entire key as prefix (safety fallback).
	return len(key)
}

// commonPrefixLen returns the length of the longest common prefix of a and b.
func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))

	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}

	return n
}
