package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// padName returns the ledger name zero-padded to LedgerNameFixedSize. Mirrors
// the encoding used by AccountKey/TransactionKey/etc. .Bytes() implementations.
func padName(name string) []byte {
	out := make([]byte, dal.LedgerNameFixedSize)
	copy(out, name)

	return out
}

func newVolumeKey(ak AccountKey, asset string) VolumeKey {
	return newColoredVolumeKey(ak, asset, "")
}

func newColoredVolumeKey(ak AccountKey, asset, color string) VolumeKey {
	base, prec := ParseAssetPrecision(asset)

	return VolumeKey{
		AccountKey:     ak,
		Asset:          asset,
		AssetBase:      base,
		AssetPrecision: prec,
		Color:          color,
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
		{"long alphanumeric base", "ABCDEFGHIJKLMNOPQ/2", "ABCDEFGHIJKLMNOPQ", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vk := newVolumeKey(AccountKey{LedgerName: "test", Account: "users:alice"}, tt.asset)

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

	vk := newVolumeKey(AccountKey{LedgerName: "test", Account: "a"}, "USD/4")

	data := vk.Bytes()
	// Expected: [ledgerName padded 64B] "a" \x00 [color=""] \x00 "USD" \x04
	expected := append(padName("test"), 'a', 0x00, 0x00, 'U', 'S', 'D', 0x04)
	require.Equal(t, expected, data)
}

func TestVolumeKey_StructLiteralFallback(t *testing.T) {
	t.Parallel()

	// VolumeKey constructed via struct literal (legacy pattern) should still work.
	vk := VolumeKey{
		AccountKey: AccountKey{LedgerName: "test", Account: "a"},
		Asset:      "EUR/2",
	}

	data := vk.Bytes()
	expected := append(padName("test"), 'a', 0x00, 0x00, 'E', 'U', 'R', 0x02)
	require.Equal(t, expected, data)
}

func TestVolumeKey_ColorByteFormat(t *testing.T) {
	t.Parallel()

	vk := newColoredVolumeKey(AccountKey{LedgerName: "test", Account: "a"}, "USD/4", "RED")

	data := vk.Bytes()
	// Expected: [ledgerName padded 64B] "a" \x00 "RED" \x00 "USD" \x04
	expected := append(padName("test"), 'a', 0x00, 'R', 'E', 'D', 0x00, 'U', 'S', 'D', 0x04)
	require.Equal(t, expected, data)
}

func TestVolumeKey_ColorRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		asset string
		color string
	}{
		{"uncolored EUR", "EUR", ""},
		{"uncolored USD/4", "USD/4", ""},
		{"colored RED USD/4", "USD/4", "RED"},
		{"colored GRANTS USD", "USD", "GRANTS"},
		{"colored long name BTC/8", "BTC/8", "TREASURY"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			vk := newColoredVolumeKey(AccountKey{LedgerName: "test", Account: "users:alice"}, tc.asset, tc.color)
			data := vk.Bytes()

			var decoded VolumeKey
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, vk, decoded)
			require.Equal(t, tc.color, decoded.Color)
		})
	}
}

// Distinct colors on the same (account, asset) must serialize to distinct
// byte sequences — the whole point of the segregation.
func TestVolumeKey_ColorSegregatesBytes(t *testing.T) {
	t.Parallel()

	ak := AccountKey{LedgerName: "test", Account: "alice"}

	uncolored := newColoredVolumeKey(ak, "USD/2", "").Bytes()
	red := newColoredVolumeKey(ak, "USD/2", "RED").Bytes()
	blue := newColoredVolumeKey(ak, "USD/2", "BLUE").Bytes()

	require.NotEqual(t, uncolored, red)
	require.NotEqual(t, uncolored, blue)
	require.NotEqual(t, red, blue)
}

func TestMetadataKey_RoundTrip(t *testing.T) {
	t.Parallel()

	mk := MetadataKey{
		AccountKey: AccountKey{LedgerName: "test", Account: "users:alice"},
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
		AccountKey: AccountKey{LedgerName: "test", Account: "a"},
		Key:        "k",
	}

	data := mk.Bytes()
	// Expected: [ledgerName padded 64B] "a" \x01 "k"
	expected := append(padName("test"), 'a', 0x01, 'k')
	require.Equal(t, expected, data)
}

func TestTransactionKey_RoundTrip(t *testing.T) {
	t.Parallel()

	tk := TransactionKey{LedgerName: "test", ID: 12345}

	data := tk.Bytes()

	var decoded TransactionKey
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, tk, decoded)
}

func TestTransactionKey_ByteFormat(t *testing.T) {
	t.Parallel()

	tk := TransactionKey{LedgerName: "test", ID: 1}

	data := tk.Bytes()
	// Expected: [ledgerName padded 64B] \x02 [txID BE 8B]
	expected := append(padName("test"),
		0x02,                                           // separator
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // txID = 1
	)
	require.Equal(t, expected, data)
}

func TestTransactionReferenceKey_RoundTrip(t *testing.T) {
	t.Parallel()

	trk := TransactionReferenceKey{LedgerName: "test", Reference: "order-abc-123"}

	data := trk.Bytes()

	var decoded TransactionReferenceKey
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, trk, decoded)
}

func TestTransactionReferenceKey_ByteFormat(t *testing.T) {
	t.Parallel()

	trk := TransactionReferenceKey{LedgerName: "test", Reference: "ref"}

	data := trk.Bytes()
	// Expected: [ledgerName padded 64B][reference]
	expected := append(padName("test"), 'r', 'e', 'f')
	require.Equal(t, expected, data)
}

func TestLedgerMetadataKey_RoundTrip(t *testing.T) {
	t.Parallel()

	lmk := LedgerMetadataKey{LedgerName: "test", Key: "description"}

	data := lmk.Bytes()

	var decoded LedgerMetadataKey
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, lmk, decoded)
}

func TestLedgerMetadataKey_ByteFormat(t *testing.T) {
	t.Parallel()

	lmk := LedgerMetadataKey{LedgerName: "test", Key: "k"}

	data := lmk.Bytes()
	// Expected: [ledgerName padded 64B] \x01 "k"
	expected := append(padName("test"), 0x01, 'k')
	require.Equal(t, expected, data)
}

func TestPreparedQueryKey_ByteFormat(t *testing.T) {
	t.Parallel()

	k := PreparedQueryKey{LedgerName: "test", Name: "q1"}

	data := k.Bytes()
	// Expected: [ledgerName padded 64B][name]
	expected := append(padName("test"), 'q', '1')
	require.Equal(t, expected, data)
}

func TestNumscriptVersionKey_ByteFormat(t *testing.T) {
	t.Parallel()

	k := NumscriptVersionKey{LedgerName: "test", Name: "pay"}

	data := k.Bytes()
	// Expected: [ledgerName padded 64B][name]
	expected := append(padName("test"), 'p', 'a', 'y')
	require.Equal(t, expected, data)
}

func TestNumscriptEntryKey_ByteFormat(t *testing.T) {
	t.Parallel()

	k := NumscriptEntryKey{LedgerName: "test", Name: "pay", Version: "v1"}

	data := k.Bytes()
	// Expected: [ledgerName padded 64B][name]\x00[version]
	expected := append(padName("test"), 'p', 'a', 'y', 0x00, 'v', '1')
	require.Equal(t, expected, data)
}
