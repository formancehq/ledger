package dal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVolumeKey_RoundTrip(t *testing.T) {
	t.Parallel()

	original := VolumeKey{
		AccountKey: AccountKey{Ledger: "ledger-42", Account: "users:alice"},
		Asset:      "USD",
	}

	data := original.Bytes()

	var decoded VolumeKey
	err := decoded.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, original.Ledger, decoded.Ledger)
	require.Equal(t, original.Account, decoded.Account)
	require.Equal(t, original.Asset, decoded.Asset)
}

func TestVolumeKey_Unmarshal_TooShort(t *testing.T) {
	t.Parallel()

	var bk VolumeKey
	err := bk.Unmarshal([]byte{1, 2})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 3 parts")
}

func TestVolumeKey_Unmarshal_MissingSeparator(t *testing.T) {
	t.Parallel()

	// data without enough null separators (needs 3 parts)
	data := []byte{'a', 'b', 'c'}
	var bk VolumeKey
	err := bk.Unmarshal(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 3 parts")
}

func TestMetadataKey_RoundTrip(t *testing.T) {
	t.Parallel()

	original := MetadataKey{
		AccountKey: AccountKey{Ledger: "ledger-10", Account: "orders:123"},
		Key:        "category",
	}

	data := original.Bytes()

	var decoded MetadataKey
	err := decoded.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, original.Ledger, decoded.Ledger)
	require.Equal(t, original.Account, decoded.Account)
	require.Equal(t, original.Key, decoded.Key)
}

func TestMetadataKey_Unmarshal_TooShort(t *testing.T) {
	t.Parallel()

	var mk MetadataKey
	err := mk.Unmarshal([]byte{1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected ledger separator")
}

func TestMetadataKey_Unmarshal_MissingSeparator(t *testing.T) {
	t.Parallel()

	// ledger + \x00 + data without \x01 separator
	data := []byte("ledger\x00abc")
	var mk MetadataKey
	err := mk.Unmarshal(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing account/key separator")
}

func TestTransactionKey_RoundTrip(t *testing.T) {
	t.Parallel()

	original := TransactionKey{Ledger: "ledger-5", ID: 12345}

	data := original.Bytes()
	require.Len(t, data, len("ledger-5")+1+8)

	var decoded TransactionKey
	err := decoded.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, original.Ledger, decoded.Ledger)
	require.Equal(t, original.ID, decoded.ID)
}

func TestTransactionKey_Unmarshal_TooShort(t *testing.T) {
	t.Parallel()

	var tk TransactionKey
	err := tk.Unmarshal([]byte{0, 0})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected [ledger]")
}

func TestIdempotencyKey_RoundTrip(t *testing.T) {
	t.Parallel()

	original := IdempotencyKey{Key: "req-abc-123"}

	data := original.Bytes()

	var decoded IdempotencyKey
	err := decoded.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, original.Key, decoded.Key)
}

func TestTransactionReferenceKey_RoundTrip(t *testing.T) {
	t.Parallel()

	original := TransactionReferenceKey{Ledger: "ledger-7", Reference: "order-42"}

	data := original.Bytes()

	var decoded TransactionReferenceKey
	err := decoded.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, original.Ledger, decoded.Ledger)
	require.Equal(t, original.Reference, decoded.Reference)
}

func TestTransactionReferenceKey_Unmarshal_TooShort(t *testing.T) {
	t.Parallel()

	var trk TransactionReferenceKey
	err := trk.Unmarshal([]byte{'a'})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 2 parts")
}

func TestLedgerKey_RoundTrip(t *testing.T) {
	t.Parallel()

	original := LedgerKey{Name: "my-ledger"}

	data := original.Bytes()

	var decoded LedgerKey
	err := decoded.Unmarshal(data)
	require.NoError(t, err)

	require.Equal(t, original.Name, decoded.Name)
}

func TestSplitNullBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		data     []byte
		n        int
		expected [][]byte
	}{
		{
			name:     "two parts",
			data:     []byte("hello\x00world"),
			n:        2,
			expected: [][]byte{[]byte("hello"), []byte("world")},
		},
		{
			name:     "no separator",
			data:     []byte("hello"),
			n:        2,
			expected: [][]byte{[]byte("hello")},
		},
		{
			name:     "multiple separators limited",
			data:     []byte("a\x00b\x00c"),
			n:        2,
			expected: [][]byte{[]byte("a"), []byte("b\x00c")},
		},
		{
			name:     "empty data",
			data:     []byte{},
			n:        2,
			expected: [][]byte{{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := splitNullBytes(tc.data, tc.n)
			require.Equal(t, tc.expected, result)
		})
	}
}
