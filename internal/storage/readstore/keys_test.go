package readstore

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/stretchr/testify/require"
)

func TestParseReverseMapKey_AccountRoundTrip(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	key := AccountReverseMapKeyV(kb, "main", "users:42", "wallet_id", 7)

	got, ok := ParseReverseMapKey(key)
	require.True(t, ok)
	require.Equal(t, "main", got.Ledger)
	require.Equal(t, NamespaceAccount, got.Namespace)
	require.Equal(t, "users:42", string(got.EntityID))
	require.Equal(t, uint32(7), got.Version)
	require.Equal(t, "wallet_id", got.MetadataKey)
}

func TestParseReverseMapKey_TransactionRoundTrip(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	key := TransactionReverseMapKeyV(kb, "main", 4242, "source", 3)

	got, ok := ParseReverseMapKey(key)
	require.True(t, ok)
	require.Equal(t, "main", got.Ledger)
	require.Equal(t, NamespaceTransaction, got.Namespace)
	require.Len(t, got.EntityID, 8, "EntityID must be exactly the 8-byte txID, not an over-read of the remaining key")
	require.Equal(t, uint64(4242), binary.BigEndian.Uint64(got.EntityID))
	require.Equal(t, uint32(3), got.Version)
	require.Equal(t, "source", got.MetadataKey)
}

func TestParseReverseMapKey_MaxLengthLedgerNameRoundTrip(t *testing.T) {
	t.Parallel()

	// Exactly dal.LedgerNameFixedSize characters: zero padding bytes, so
	// bytes.TrimRight is a no-op. This is the boundary a LedgerNameFixedSize
	// regression (e.g. an off-by-one in the fixed-width block) would break;
	// both round-trip tests above use a short name and never exercise it.
	maxLedgerName := strings.Repeat("a", dal.LedgerNameFixedSize)

	kb := dal.NewKeyBuilder()
	key := AccountReverseMapKeyV(kb, maxLedgerName, "users:1", "wallet_id", 1)

	got, ok := ParseReverseMapKey(key)
	require.True(t, ok)
	require.Equal(t, maxLedgerName, got.Ledger)
	require.Equal(t, NamespaceAccount, got.Namespace)
	require.Equal(t, "users:1", string(got.EntityID))
	require.Equal(t, uint32(1), got.Version)
	require.Equal(t, "wallet_id", got.MetadataKey)
}

func TestParseReverseMapKey_Rejects(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	truncatedBeforeNamespace := append([]byte{PrefixReverseMap}, make([]byte, dal.LedgerNameFixedSize)...)

	// Embedded NUL inside the ledger-name block: "main\x00JUNK" followed by
	// zero padding, with an otherwise well-formed account tail. A valid
	// ledger name cannot contain a NUL, so bytes.TrimRight leaving one behind
	// must be treated as corruption, not accepted as ledger name "main".
	// PutLedgerNameFixed copies the string's raw bytes (including the
	// embedded NUL) then zero-pads the remainder.
	embeddedNulLedgerName := dal.NewKeyBuilder().
		Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed("main\x00JUNK").
		PutNamespace(NamespaceAccount).
		PutStringNull("users:1").
		PutUint32(1).
		PutString("meta").
		Build()

	// Unknown namespace: a valid-looking header followed by NamespaceLog
	// ("l:") instead of NamespaceAccount/NamespaceTransaction.
	unknownNamespace := dal.NewKeyBuilder().
		Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed("main").
		PutNamespace(NamespaceLog).
		PutStringNull("some-entity").
		PutUint32(1).
		PutString("meta").
		Build()

	// Account key missing the 0x00 terminator after the address: build the
	// header + namespace by hand, then append the account bytes with no
	// terminator at all.
	accountNoTerminator := dal.NewKeyBuilder().
		Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed("main").
		PutNamespace(NamespaceAccount).
		Build()
	accountNoTerminator = append(accountNoTerminator, []byte("users:1")...)

	// Transaction key truncated inside the version block: txID present,
	// fewer than 4 bytes after it.
	txTruncatedVersion := dal.NewKeyBuilder().
		Reset().
		PutByte(PrefixReverseMap).
		PutLedgerNameFixed("main").
		PutNamespace(NamespaceTransaction).
		PutUint64(4242).
		Build()
	txTruncatedVersion = append(txTruncatedVersion, []byte{0x00, 0x01}...)

	tests := map[string]struct {
		key []byte
	}{
		"nil key must be rejected": {
			key: nil,
		},
		"empty key must be rejected": {
			key: []byte{},
		},
		"wrong prefix byte must be rejected": {
			// Own KeyBuilder: MetadataIndexKeyV returns via Consume(), which
			// aliases the builder's backing array — sharing "kb" with the
			// other table entries below would let a later Build()/Reset()
			// on the same builder silently overwrite these bytes.
			key: MetadataIndexKeyV(dal.NewKeyBuilder(), "main", NamespaceAccount, "wallet_id", 1, []byte{0x01}, []byte("users:1")),
		},
		"key truncated before the namespace must be rejected": {
			key: truncatedBeforeNamespace,
		},
		"ledger-name block with an embedded NUL must be rejected": {
			key: embeddedNulLedgerName,
		},
		"unknown namespace must be rejected": {
			key: unknownNamespace,
		},
		"account key without a 0x00 terminator after the address must be rejected": {
			key: accountNoTerminator,
		},
		"transaction key truncated inside the version block must be rejected": {
			key: txTruncatedVersion,
		},
		"empty metadata key must be rejected": {
			key: AccountReverseMapKeyV(kb, "main", "users:1", "", 1),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, ok := ParseReverseMapKey(tt.key)
			require.False(t, ok, "malformed key must be rejected, never silently accepted: %s", name)
		})
	}
}
