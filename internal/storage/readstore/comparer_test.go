package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// ledgerIDKey builds a test key: [prefix][ledgerID BE 4B][suffix...].
func ledgerIDKey(prefix byte, ledgerID uint32, suffix ...byte) []byte {
	key := make([]byte, 1+4+len(suffix))
	key[0] = prefix
	binary.BigEndian.PutUint32(key[1:], ledgerID)
	copy(key[5:], suffix)

	return key
}

func TestReadStoreSplit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       []byte
		wantSplit int
	}{
		{
			name:      "metadata index with ledger ID 1",
			key:       ledgerIDKey(PrefixMetadataIndex, 1, 'a', ':', 'm', 'e', 't', 'a'),
			wantSplit: 5, // [0x01][00 00 00 01]
		},
		{
			name:      "entity exists with ledger ID 256",
			key:       ledgerIDKey(PrefixEntityExists, 256, 'a', ':', 'k'),
			wantSplit: 5, // [0x02][00 00 01 00]
		},
		{
			name:      "account tx with ledger ID 42 and suffix",
			key:       ledgerIDKey(PrefixAccountTx, 42, 'a', 'c', 'c', 0x00, 1, 2, 3, 4, 5, 6, 7, 8),
			wantSplit: 5, // [0x04][00 00 00 2A]
		},
		{
			name:      "ledger ID with zero bytes (ID=0)",
			key:       ledgerIDKey(PrefixMetadataIndex, 0, 'x'),
			wantSplit: 5, // must not be confused by 0x00 bytes in the ID
		},
		{
			name:      "large ledger ID (0x01020304)",
			key:       ledgerIDKey(PrefixTransactionTimestamp, 0x01020304, 0x00, 0x00, 0x00, 0x01),
			wantSplit: 5,
		},
		{
			name:      "prefix-only key (exactly 5 bytes)",
			key:       ledgerIDKey(PrefixMetadataIndex, 1),
			wantSplit: 5,
		},
		{
			name:      "internal progress singleton",
			key:       []byte{PrefixInternal, SubInternalProgress},
			wantSplit: 2, // full key
		},
		{
			name:      "internal audit progress singleton",
			key:       []byte{PrefixInternal, SubInternalAuditProgress},
			wantSplit: 2, // full key
		},
		{
			name:      "backfill with ledger",
			key:       []byte{PrefixInternal, SubInternalBackfill, 0x00, 0x00, 0x00, 0x01, 'b', 0x01},
			wantSplit: 8, // full key — internal prefix, no bloom split
		},
		{
			name:      "empty key",
			key:       []byte{},
			wantSplit: 0,
		},
		{
			name:      "single byte",
			key:       []byte{0x01},
			wantSplit: 1,
		},
		{
			name:      "short key (3 bytes, less than ledgerScopedPrefixLen)",
			key:       []byte{0x01, 0x00, 0x00},
			wantSplit: 3, // fallback: entire key
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := readStoreSplit(tt.key)
			require.Equal(t, tt.wantSplit, got)
		})
	}
}

func TestReadStoreSplit_DistinctLedgerIDs(t *testing.T) {
	t.Parallel()

	// Verify that different ledger IDs produce different split prefixes,
	// even when the uint32 bytes contain 0x00.
	split := ReadStoreComparer.Split

	key1 := ledgerIDKey(PrefixMetadataIndex, 1, 'a', ':', 'x')
	key2 := ledgerIDKey(PrefixMetadataIndex, 2, 'a', ':', 'x')
	key256 := ledgerIDKey(PrefixMetadataIndex, 256, 'a', ':', 'x')

	prefix1 := key1[:split(key1)]
	prefix2 := key2[:split(key2)]
	prefix256 := key256[:split(key256)]

	require.NotEqual(t, prefix1, prefix2, "ledger 1 and 2 must have distinct prefixes")
	require.NotEqual(t, prefix1, prefix256, "ledger 1 and 256 must have distinct prefixes")
	require.NotEqual(t, prefix2, prefix256, "ledger 2 and 256 must have distinct prefixes")
}

func TestReadStoreComparerOrdering(t *testing.T) {
	t.Parallel()

	// Verify that the custom comparer produces the same ordering as bytes.Compare.
	keys := [][]byte{
		ledgerIDKey(0x01, 1, 'x'),           // [0x01][00 00 00 01][x]
		ledgerIDKey(0x01, 1, 'y'),           // [0x01][00 00 00 01][y]
		ledgerIDKey(0x01, 2, 'x'),           // [0x01][00 00 00 02][x]
		ledgerIDKey(0x08, 1, 0x01),          // [0x08][00 00 00 01][01]
		{PrefixInternal, SubInternalProgress},      // [0xFE][0x01]
		{PrefixInternal, SubInternalAuditProgress}, // [0xFE][0x02]
	}

	cmp := ReadStoreComparer.Compare
	for i := range len(keys) - 1 {
		for j := i + 1; j < len(keys); j++ {
			result := cmp(keys[i], keys[j])
			require.Equal(t, -1, result,
				"Compare(%x, %x) should be -1, got %d", keys[i], keys[j], result)
		}
	}
}

func TestReadStoreComparerImmediateSuccessor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{
			name:   "ledger-scoped prefix (ID=1)",
			prefix: ledgerIDKey(0x01, 1),
			want:   ledgerIDKey(0x01, 2),
		},
		{
			name:   "ledger-scoped prefix (ID=255)",
			prefix: ledgerIDKey(0x01, 255),
			want:   ledgerIDKey(0x01, 256),
		},
		{
			name:   "internal singleton prefix",
			prefix: []byte{PrefixInternal, SubInternalProgress},
			want:   []byte{PrefixInternal, SubInternalProgress, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ReadStoreComparer.ImmediateSuccessor(nil, tt.prefix)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestReadStoreComparerSplitProperties(t *testing.T) {
	t.Parallel()

	// Verify Split properties from the Pebble docs:
	// 1. A prefix-only key sorts before keys with that prefix + suffix
	// 2. If Compare(a,b) <= 0 then Compare(prefix(a), prefix(b)) <= 0

	cmp := ReadStoreComparer.Compare
	split := ReadStoreComparer.Split

	pairs := [][2][]byte{
		{
			ledgerIDKey(0x01, 1),                    // prefix-only
			ledgerIDKey(0x01, 1, 'a', ':', 'x'),     // prefix + suffix
		},
		{
			ledgerIDKey(0x01, 1, 'x'),
			ledgerIDKey(0x01, 2, 'y'),
		},
	}

	for _, pair := range pairs {
		a, b := pair[0], pair[1]
		if cmp(a, b) > 0 {
			a, b = b, a
		}

		prefixA := a[:split(a)]
		prefixB := b[:split(b)]
		require.LessOrEqual(t, cmp(prefixA, prefixB), 0,
			"Compare(prefix(%x), prefix(%x)) should be <= 0", a, b)
	}
}
