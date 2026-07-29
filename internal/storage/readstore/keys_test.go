package readstore

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestParseReverseMapKey_AccountRoundTrip(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	key := AccountReverseMapKeyV(kb, "main", "users:42", "wallet_id", 7)

	got, err := ParseReverseMapKey(key)
	require.NoError(t, err)
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

	got, err := ParseReverseMapKey(key)
	require.NoError(t, err)
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

	got, err := ParseReverseMapKey(key)
	require.NoError(t, err)
	require.Equal(t, maxLedgerName, got.Ledger)
	require.Equal(t, NamespaceAccount, got.Namespace)
	require.Equal(t, "users:1", string(got.EntityID))
	require.Equal(t, uint32(1), got.Version)
	require.Equal(t, "wallet_id", got.MetadataKey)
}

// TestParseReverseMapKey_Rejects covers every malformed-key shape
// ParseReverseMapKey must reject. Convention: every entry builds its own
// fresh dal.NewKeyBuilder() rather than sharing one across entries. Several
// of the key builders in this package (e.g. MetadataIndexKeyV) hand back
// their result via KeyBuilder.Consume(), which aliases the builder's
// backing array instead of copying it; reusing a single builder across
// multiple table entries would let a later Build()/Reset() on that builder
// silently overwrite an earlier entry's bytes before its subtest runs. A
// fresh builder per entry sidesteps the hazard entirely instead of relying
// on every future entry to reason about which method the builder it reuses
// happens to return through.
func TestParseReverseMapKey_Rejects(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		key     []byte
		wantErr error
	}{
		"nil key must be rejected": {
			key:     nil,
			wantErr: ErrReverseMapKeyPrefix,
		},
		"empty key must be rejected": {
			key:     []byte{},
			wantErr: ErrReverseMapKeyPrefix,
		},
		"wrong prefix byte must be rejected": {
			key:     MetadataIndexKeyV(dal.NewKeyBuilder(), "main", NamespaceAccount, "wallet_id", 1, []byte{0x01}, []byte("users:1")),
			wantErr: ErrReverseMapKeyPrefix,
		},
		"key truncated before the namespace must be rejected": {
			key:     append([]byte{PrefixReverseMap}, make([]byte, dal.LedgerNameFixedSize)...),
			wantErr: ErrReverseMapKeyTruncated,
		},
		"ledger-name block with an embedded NUL must be rejected": {
			// "main\x00JUNK" followed by zero padding, with an otherwise
			// well-formed account tail. A valid ledger name cannot contain a
			// NUL, so bytes.TrimRight leaving one behind must be treated as
			// corruption, not accepted as ledger name "main".
			// PutLedgerNameFixed copies the string's raw bytes (including the
			// embedded NUL) then zero-pads the remainder.
			key: dal.NewKeyBuilder().
				Reset().
				PutByte(PrefixReverseMap).
				PutLedgerNameFixed("main\x00JUNK").
				PutNamespace(NamespaceAccount).
				PutStringNull("users:1").
				PutUint32(1).
				PutString("meta").
				Build(),
			wantErr: ErrReverseMapKeyLedgerName,
		},
		"ledger-name block that is all zero padding must be rejected": {
			// An otherwise well-formed account key whose ledger-name block
			// trims to the empty string. No stored ledger is ever named "",
			// so this is corruption, not a valid zero-length name.
			key: dal.NewKeyBuilder().
				Reset().
				PutByte(PrefixReverseMap).
				PutLedgerNameFixed("").
				PutNamespace(NamespaceAccount).
				PutStringNull("users:1").
				PutUint32(1).
				PutString("meta").
				Build(),
			wantErr: ErrReverseMapKeyLedgerName,
		},
		"unknown namespace must be rejected": {
			// A valid-looking header followed by NamespaceLog ("l:") instead
			// of NamespaceAccount/NamespaceTransaction.
			key: dal.NewKeyBuilder().
				Reset().
				PutByte(PrefixReverseMap).
				PutLedgerNameFixed("main").
				PutNamespace(NamespaceLog).
				PutStringNull("some-entity").
				PutUint32(1).
				PutString("meta").
				Build(),
			wantErr: ErrReverseMapKeyNamespace,
		},
		"account key without a 0x00 terminator after the address must be rejected": {
			// Header + namespace built by hand, then the account bytes
			// appended with no terminator at all.
			key: append(
				dal.NewKeyBuilder().
					Reset().
					PutByte(PrefixReverseMap).
					PutLedgerNameFixed("main").
					PutNamespace(NamespaceAccount).
					Build(),
				[]byte("users:1")...,
			),
			wantErr: ErrReverseMapKeyTruncated,
		},
		"account key with an empty entity id must be rejected": {
			key:     AccountReverseMapKeyV(dal.NewKeyBuilder(), "main", "", "wallet_id", 1),
			wantErr: ErrReverseMapKeyEntityID,
		},
		"account entity id with an embedded NUL must be rejected, not silently mis-decoded": {
			// The critical regression case: an account address like
			// "us\x00ers" makes the 0x00 terminator search above stop at the
			// FIRST byte, so the *real* terminator and the 4-byte version
			// block get swallowed into what would otherwise decode as
			// MetadataKey (a corrupted-looking, NUL-containing string) —
			// a plausible but wrong (EntityID, Version, MetadataKey) triple
			// pointing at a different entity than the key actually encodes,
			// rather than an outright decode failure. Must be rejected as
			// ErrReverseMapKeyMetadataKey, never accepted as consistent.
			key:     AccountReverseMapKeyV(dal.NewKeyBuilder(), "main", "us\x00ers", "wallet_id", 1),
			wantErr: ErrReverseMapKeyMetadataKey,
		},
		"metadata key containing a NUL byte must be rejected directly": {
			// A well-formed account tail (proper terminator, proper 4-byte
			// version) whose metadata-key tail itself contains a raw NUL —
			// isolates the MetadataKey NUL check from the entity-id mis-split
			// cascade covered by the case above.
			key: dal.NewKeyBuilder().
				Reset().
				PutByte(PrefixReverseMap).
				PutLedgerNameFixed("main").
				PutNamespace(NamespaceAccount).
				PutStringNull("users:1").
				PutUint32(1).
				PutString("meta\x00key").
				Build(),
			wantErr: ErrReverseMapKeyMetadataKey,
		},
		"empty metadata key must be rejected for an account entity": {
			key:     AccountReverseMapKeyV(dal.NewKeyBuilder(), "main", "users:1", "", 1),
			wantErr: ErrReverseMapKeyMetadataKey,
		},
		"empty metadata key must be rejected for a transaction entity": {
			key:     TransactionReverseMapKeyV(dal.NewKeyBuilder(), "main", 4242, "", 1),
			wantErr: ErrReverseMapKeyMetadataKey,
		},
		"transaction id shorter than 8 bytes must be rejected": {
			// Header + namespace only, then 5 bytes — not enough for the
			// 8-byte txID EntityID extraction to even begin.
			key: append(
				dal.NewKeyBuilder().
					Reset().
					PutByte(PrefixReverseMap).
					PutLedgerNameFixed("main").
					PutNamespace(NamespaceTransaction).
					Build(),
				[]byte{0x00, 0x00, 0x00, 0x00, 0x00}...,
			),
			wantErr: ErrReverseMapKeyTruncated,
		},
		"transaction key truncated well inside the version block must be rejected": {
			key: append(
				dal.NewKeyBuilder().
					Reset().
					PutByte(PrefixReverseMap).
					PutLedgerNameFixed("main").
					PutNamespace(NamespaceTransaction).
					PutUint64(4242).
					Build(),
				[]byte{0x00, 0x01}...,
			),
			wantErr: ErrReverseMapKeyTruncated,
		},
		"transaction key exactly 3 bytes short of the 4-byte version minimum must be rejected": {
			// Pins the len(rest) < 4 boundary precisely: mutating that guard
			// to < 3 would still fail every other truncation case in this
			// table (they all leave 0-2 bytes), but would wrongly let this
			// 3-byte tail through into a rest[:4] out-of-bounds read.
			key: append(
				dal.NewKeyBuilder().
					Reset().
					PutByte(PrefixReverseMap).
					PutLedgerNameFixed("main").
					PutNamespace(NamespaceTransaction).
					PutUint64(4242).
					Build(),
				[]byte{0x00, 0x00, 0x00}...,
			),
			wantErr: ErrReverseMapKeyTruncated,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseReverseMapKey(tt.key)
			require.Error(t, err, "malformed key must be rejected, never silently accepted: %s", name)
			require.ErrorIs(t, err, tt.wantErr, name)
		})
	}
}
