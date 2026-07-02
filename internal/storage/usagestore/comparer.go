package usagestore

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// usageStoreComparerName is persisted in the Pebble database. Changing it
// requires rebuilding the usage store from the Raft log.
const usageStoreComparerName = "formance.usagestore.v1"

// ledgerScopedPrefixLen is the split point for ledger-scoped keys:
// 1 byte prefix + LedgerNameFixedSize bytes ledger name (zero-padded).
const ledgerScopedPrefixLen = 1 + dal.LedgerNameFixedSize

// UsageStoreComparer splits keys at the [prefix_byte][ledger_name padded 64B]
// boundary so that bloom filters are built on the ledger-scoped prefix rather
// than the full key. Mirrors readstore's comparer semantics — see that file
// for the full rationale.
var UsageStoreComparer = &pebble.Comparer{
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

	Split: usageStoreSplit,

	ImmediateSuccessor: func(dst, prefix []byte) []byte {
		dst = append(dst[:0], prefix...)
		if len(dst) == ledgerScopedPrefixLen {
			dst[len(dst)-1]++

			return dst
		}

		return append(dst, 0x00)
	},

	Name: usageStoreComparerName,
}

// usageStoreSplit returns the split point for bloom filter prefix extraction.
func usageStoreSplit(key []byte) int {
	if len(key) <= 1 {
		return len(key)
	}

	// Internal singleton keys (non-ledger-scoped) — full key is the prefix.
	if key[0] == PrefixInternal {
		return len(key)
	}

	// Ledger-scoped keys: [prefix_byte][ledger_name padded 64B][...].
	if len(key) >= ledgerScopedPrefixLen {
		return ledgerScopedPrefixLen
	}

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
