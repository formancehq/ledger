package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ledgerKey builds a test key: [prefix][ledgerName padded 64B][suffix...].
func ledgerKey(prefix byte, ledgerName string, suffix ...byte) []byte {
	key := make([]byte, 1+dal.LedgerNameFixedSize+len(suffix))
	key[0] = prefix
	copy(key[1:1+dal.LedgerNameFixedSize], ledgerName)
	copy(key[1+dal.LedgerNameFixedSize:], suffix)

	return key
}

// expectedSplit is the canonical split point for ledger-scoped readstore keys.
const expectedSplit = 1 + dal.LedgerNameFixedSize

func TestReadStoreSplit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       []byte
		wantSplit int
	}{
		{
			name:      "metadata index with ledger 'a'",
			key:       ledgerKey(PrefixMetadataIndex, "a", 'a', ':', 'm', 'e', 't', 'a'),
			wantSplit: expectedSplit,
		},
		{
			name:      "entity exists with ledger 'foo'",
			key:       ledgerKey(PrefixEntityExists, "foo", 'a', ':', 'k'),
			wantSplit: expectedSplit,
		},
		{
			name:      "account tx with suffix",
			key:       ledgerKey(PrefixAccountTx, "billing", 'a', 'c', 'c', 0x00, 1, 2, 3, 4, 5, 6, 7, 8),
			wantSplit: expectedSplit,
		},
		{
			name:      "empty ledger name (all zero pad)",
			key:       ledgerKey(PrefixMetadataIndex, "", 'x'),
			wantSplit: expectedSplit,
		},
		{
			name:      "long ledger name",
			key:       ledgerKey(PrefixTransactionTimestamp, "very-long-ledger-name", 0x00, 0x00, 0x00, 0x01),
			wantSplit: expectedSplit,
		},
		{
			name:      "prefix-only key (exactly prefix+name)",
			key:       ledgerKey(PrefixMetadataIndex, "a"),
			wantSplit: expectedSplit,
		},
		{
			name:      "internal progress singleton",
			key:       []byte{PrefixInternal, SubInternalProgress},
			wantSplit: 2, // full key
		},
		{
			name:      "internal audit progress singleton",
			key:       []byte{PrefixInternal, SubInternalAppliedProposalProgress},
			wantSplit: 2, // full key
		},
		{
			name:      "backfill with ledger",
			key:       append([]byte{PrefixInternal, SubInternalBackfill}, append(make([]byte, dal.LedgerNameFixedSize), 'b', 0x01)...),
			wantSplit: 2 + dal.LedgerNameFixedSize + 2, // full key — internal prefix, no bloom split
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
			name:      "short key (less than ledgerScopedPrefixLen)",
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

func TestReadStoreSplit_DistinctLedgerNames(t *testing.T) {
	t.Parallel()

	// Verify that different ledger names produce different split prefixes.
	split := ReadStoreComparer.Split

	keyA := ledgerKey(PrefixMetadataIndex, "a", 'a', ':', 'x')
	keyB := ledgerKey(PrefixMetadataIndex, "b", 'a', ':', 'x')
	keyLong := ledgerKey(PrefixMetadataIndex, "billing", 'a', ':', 'x')

	prefixA := keyA[:split(keyA)]
	prefixB := keyB[:split(keyB)]
	prefixLong := keyLong[:split(keyLong)]

	require.NotEqual(t, prefixA, prefixB, "ledgers a and b must have distinct prefixes")
	require.NotEqual(t, prefixA, prefixLong, "ledgers a and billing must have distinct prefixes")
	require.NotEqual(t, prefixB, prefixLong, "ledgers b and billing must have distinct prefixes")
}

func TestReadStoreComparerOrdering(t *testing.T) {
	t.Parallel()

	// Verify that the custom comparer produces the same ordering as bytes.Compare.
	keys := [][]byte{
		ledgerKey(0x01, "a", 'x'),
		ledgerKey(0x01, "a", 'y'),
		ledgerKey(0x01, "b", 'x'),
		ledgerKey(0x08, "a", 0x01),
		{PrefixInternal, SubInternalProgress},
		{PrefixInternal, SubInternalAppliedProposalProgress},
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
			name:   "ledger-scoped prefix ('a' → 'b')",
			prefix: ledgerKey(0x01, "a"),
			want: func() []byte {
				out := ledgerKey(0x01, "a")
				out[len(out)-1]++

				return out
			}(),
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
			ledgerKey(0x01, "a"),                // prefix-only
			ledgerKey(0x01, "a", 'a', ':', 'x'), // prefix + suffix
		},
		{
			ledgerKey(0x01, "a", 'x'),
			ledgerKey(0x01, "b", 'y'),
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
