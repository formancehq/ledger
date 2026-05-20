package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newVolumeKey(ak AccountKey, asset string) VolumeKey {
	base, prec := ParseAssetPrecision(asset)

	return VolumeKey{
		AccountKey:     ak,
		Asset:          asset,
		AssetBase:      base,
		AssetPrecision: prec,
	}
}

func TestSinkConfigKey_Bytes(t *testing.T) {
	t.Parallel()

	k := SinkConfigKey{Name: "my-sink"}
	require.Equal(t, []byte("my-sink"), k.Bytes())
}

func TestVolumeKey_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		asset    string
		wantBase string
		wantPrec uint8
	}{
		{"with precision", "USD/4", "USD", 4},
		{"zero precision", "EUR", "EUR", 0},
		{"high precision", "BTC/8", "BTC", 8},
		{"underscore asset", "CUSTOM_TOKEN/2", "CUSTOM_TOKEN", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vk := newVolumeKey(AccountKey{LedgerID: 1, Account: "users:alice"}, tt.asset)

			data := vk.Bytes()

			var decoded VolumeKey
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, vk, decoded)
			require.Equal(t, tt.asset, decoded.Asset)
			require.Equal(t, tt.wantBase, decoded.AssetBase)
			require.Equal(t, tt.wantPrec, decoded.AssetPrecision)
		})
	}
}

func TestVolumeKey_ByteFormat(t *testing.T) {
	t.Parallel()

	vk := newVolumeKey(AccountKey{LedgerID: 1, Account: "a"}, "USD/4")

	data := vk.Bytes()
	// Expected: [ledgerID BE 4B] "a" \x00 "USD" \x04
	expected := []byte{0x00, 0x00, 0x00, 0x01, 'a', 0x00, 'U', 'S', 'D', 0x04}
	require.Equal(t, expected, data)
}

func TestVolumeKey_StructLiteralFallback(t *testing.T) {
	t.Parallel()

	// VolumeKey constructed via struct literal (legacy pattern) should still work.
	vk := VolumeKey{
		AccountKey: AccountKey{LedgerID: 1, Account: "a"},
		Asset:      "EUR/2",
	}

	data := vk.Bytes()
	expected := []byte{0x00, 0x00, 0x00, 0x01, 'a', 0x00, 'E', 'U', 'R', 0x02}
	require.Equal(t, expected, data)
}

func TestMetadataKey_RoundTrip(t *testing.T) {
	t.Parallel()

	mk := MetadataKey{
		AccountKey: AccountKey{LedgerID: 42, Account: "users:alice"},
		Key:        "role",
	}

	data := mk.Bytes()

	var decoded MetadataKey
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, mk, decoded)
}

func TestMetadataKey_ByteFormat(t *testing.T) {
	t.Parallel()

	mk := MetadataKey{
		AccountKey: AccountKey{LedgerID: 1, Account: "a"},
		Key:        "k",
	}

	data := mk.Bytes()
	// Expected: [ledgerID BE 4B] "a" \x01 "k"
	expected := []byte{0x00, 0x00, 0x00, 0x01, 'a', 0x01, 'k'}
	require.Equal(t, expected, data)
}

func TestTransactionKey_RoundTrip(t *testing.T) {
	t.Parallel()

	tk := TransactionKey{LedgerID: 5, ID: 12345}

	data := tk.Bytes()

	var decoded TransactionKey
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, tk, decoded)
}

func TestTransactionKey_ByteFormat(t *testing.T) {
	t.Parallel()

	tk := TransactionKey{LedgerID: 1, ID: 1}

	data := tk.Bytes()
	// Expected: [ledgerID BE 4B] \x02 [txID BE 8B]
	expected := []byte{
		0x00, 0x00, 0x00, 0x01, // ledgerID = 1
		0x02,                                           // separator
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // txID = 1
	}
	require.Equal(t, expected, data)
}

func TestTransactionReferenceKey_RoundTrip(t *testing.T) {
	t.Parallel()

	trk := TransactionReferenceKey{LedgerID: 10, Reference: "order-abc-123"}

	data := trk.Bytes()

	var decoded TransactionReferenceKey
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, trk, decoded)
}

func TestTransactionReferenceKey_ByteFormat(t *testing.T) {
	t.Parallel()

	trk := TransactionReferenceKey{LedgerID: 1, Reference: "ref"}

	data := trk.Bytes()
	// Expected: [ledgerID BE 4B][reference]
	expected := []byte{0x00, 0x00, 0x00, 0x01, 'r', 'e', 'f'}
	require.Equal(t, expected, data)
}

func TestLedgerMetadataKey_RoundTrip(t *testing.T) {
	t.Parallel()

	lmk := LedgerMetadataKey{LedgerID: 7, Key: "description"}

	data := lmk.Bytes()

	var decoded LedgerMetadataKey
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, lmk, decoded)
}

func TestLedgerMetadataKey_ByteFormat(t *testing.T) {
	t.Parallel()

	lmk := LedgerMetadataKey{LedgerID: 1, Key: "k"}

	data := lmk.Bytes()
	// Expected: [ledgerID BE 4B] \x01 "k"
	expected := []byte{0x00, 0x00, 0x00, 0x01, 0x01, 'k'}
	require.Equal(t, expected, data)
}

func TestPreparedQueryKey_ByteFormat(t *testing.T) {
	t.Parallel()

	k := PreparedQueryKey{LedgerID: 2, Name: "q1"}

	data := k.Bytes()
	// Expected: [ledgerID BE 4B][name]
	expected := []byte{0x00, 0x00, 0x00, 0x02, 'q', '1'}
	require.Equal(t, expected, data)
}

func TestNumscriptVersionKey_ByteFormat(t *testing.T) {
	t.Parallel()

	k := NumscriptVersionKey{LedgerID: 3, Name: "pay"}

	data := k.Bytes()
	// Expected: [ledgerID BE 4B][name]
	expected := []byte{0x00, 0x00, 0x00, 0x03, 'p', 'a', 'y'}
	require.Equal(t, expected, data)
}

func TestNumscriptEntryKey_ByteFormat(t *testing.T) {
	t.Parallel()

	k := NumscriptEntryKey{LedgerID: 4, Name: "pay", Version: "v1"}

	data := k.Bytes()
	// Expected: [ledgerID BE 4B][name]\x00[version]
	expected := []byte{0x00, 0x00, 0x00, 0x04, 'p', 'a', 'y', 0x00, 'v', '1'}
	require.Equal(t, expected, data)
}
