package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadStoreSplit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      []byte
		wantSplit int
	}{
		{
			name:      "metadata index with ledger",
			key:       []byte{PrefixMetadataIndex, 'f', 'o', 'o', 0x00, 'a', ':', 'm', 'e', 't', 'a'},
			wantSplit: 5, // [0x01][foo\x00]
		},
		{
			name:      "entity exists with ledger",
			key:       []byte{PrefixEntityExists, 'b', 'a', 'r', 0x00, 'a', ':', 'k'},
			wantSplit: 5, // [0x02][bar\x00]
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
			key:       []byte{PrefixBackfill, 'l', 'e', 'd', 0x00, 'b', 0x01},
			wantSplit: 5, // [0xF1][led\x00]
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
			name:      "account tx with ledger and account",
			key:       []byte{PrefixAccountTx, 'l', 0x00, 'a', 'c', 'c', 0x00, 1, 2, 3, 4, 5, 6, 7, 8},
			wantSplit: 3, // [0x04][l\x00] — splits at first null after pos 1
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

func TestReadStoreComparerOrdering(t *testing.T) {
	t.Parallel()

	// Verify that the custom comparer produces the same ordering as bytes.Compare.
	keys := [][]byte{
		{0x01, 'a', 0x00, 'x'},
		{0x01, 'a', 0x00, 'y'},
		{0x01, 'b', 0x00, 'x'},
		{0x08, 'a', 0x00, 0x01},
		{PrefixInternal, SubInternalProgress},
		{PrefixInternal, SubInternalAuditProgress},
	}

	cmp := ReadStoreComparer.Compare
	for i := 0; i < len(keys)-1; i++ {
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
			name:   "ledger-scoped prefix ending in null",
			prefix: []byte{0x01, 'f', 'o', 'o', 0x00},
			want:   []byte{0x01, 'f', 'o', 'o', 0x01},
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
			{0x01, 'f', 'o', 'o', 0x00},           // prefix-only
			{0x01, 'f', 'o', 'o', 0x00, 'a', ':'}, // prefix + suffix
		},
		{
			{0x01, 'a', 0x00, 'x'},
			{0x01, 'b', 0x00, 'y'},
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
