package readstore

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestParseBackfillKey covers ParseBackfillKey's contract, including the
// parseLedgerNameFixed sharing introduced alongside ParseReverseMapKey: a
// public function's behaviour changed (a NUL-corrupted ledger-name block is
// now rejected instead of silently truncated) with no prior test coverage
// at all, so this pins the full contract rather than just the delta.
func TestParseBackfillKey(t *testing.T) {
	t.Parallel()

	// buildBackfillKey mirrors the format ParseBackfillKey documents:
	// [ledgerName padded 64B][kind][details]. It never includes the
	// PrefixBackfill discriminator byte — ParseBackfillKey's doc comment is
	// explicit that the caller strips that before calling it.
	buildBackfillKey := func(name string, kind byte, details []byte) []byte {
		return dal.NewKeyBuilder().
			Reset().
			PutLedgerNameFixed(name).
			PutByte(kind).
			PutBytes(details).
			Build()
	}

	maxLedgerName := strings.Repeat("a", dal.LedgerNameFixedSize)

	tests := map[string]struct {
		key         []byte
		wantLedger  string
		wantKind    byte
		wantDetails []byte
		wantErr     error
	}{
		"well-formed short zero-padded name is accepted": {
			key:         buildBackfillKey("main", BackfillKindTxBuiltin, []byte{0x01}),
			wantLedger:  "main",
			wantKind:    BackfillKindTxBuiltin,
			wantDetails: []byte{0x01},
		},
		"max-length unpadded name is still accepted": {
			key:         buildBackfillKey(maxLedgerName, BackfillKindAcctMetadata, []byte("wallet_id")),
			wantLedger:  maxLedgerName,
			wantKind:    BackfillKindAcctMetadata,
			wantDetails: []byte("wallet_id"),
		},
		"empty details is accepted": {
			key:        buildBackfillKey("main", BackfillKindLogBuiltin, nil),
			wantLedger: "main",
			wantKind:   BackfillKindLogBuiltin,
			// A zero-length slice, not nil: details is key[len(key):], the
			// tail of the same backing array — testify's require.Equal
			// distinguishes a nil []byte from a non-nil empty one.
			wantDetails: []byte{},
		},
		"NUL-corrupted ledger-name block is rejected, not silently truncated": {
			// Before parseLedgerNameFixed was shared with ParseReverseMapKey,
			// this decoded to ledger name "main" (truncate-at-first-NUL). It
			// must now be rejected like the reverse-map decoder rejects it.
			key:     buildBackfillKey("main\x00JUNK", BackfillKindTxBuiltin, []byte{0x01}),
			wantErr: errLedgerNameFixedCorrupt,
		},
		"key shorter than the fixed ledger-name block plus kind byte is rejected": {
			key:     make([]byte, dal.LedgerNameFixedSize),
			wantErr: ErrBackfillKeyTruncated,
		},
		"nil key is rejected": {
			key:     nil,
			wantErr: ErrBackfillKeyTruncated,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ledgerName, kind, details, err := ParseBackfillKey(tt.key)
			if tt.wantErr != nil {
				require.Error(t, err, name)
				require.ErrorIs(t, err, tt.wantErr, name)

				return
			}

			require.NoError(t, err, name)
			require.Equal(t, tt.wantLedger, ledgerName)
			require.Equal(t, tt.wantKind, kind)
			require.Equal(t, tt.wantDetails, details)
		})
	}
}

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

func TestParseReverseMapKey_EntityIDIsIndependentOfSourceKey(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	key := AccountReverseMapKeyV(kb, "main", "users:42", "wallet_id", 1)

	got, err := ParseReverseMapKey(key)
	require.NoError(t, err)

	wantEntityID := append([]byte(nil), got.EntityID...)

	// Overwrite the entire source key in place. If EntityID aliased key (a
	// plain sub-slice) rather than being a bytes.Clone copy, this would
	// corrupt it too — exactly the use-after-iterator-move hazard the doc
	// comment on ParseReverseMapKey promises callers are safe from, and
	// which the checker pass that scans the whole 0x03 keyspace will rely
	// on for every entity it retains past the iterator's next Next().
	for i := range key {
		key[i] = 0xFF
	}

	require.Equal(t, wantEntityID, got.EntityID, "EntityID must be independent of the source key")
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
// of the key builders in this package (e.g. MetadataIndexEventKeyV) hand back
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
		// wantErrContains pins the distinguishing context wrapped around a
		// shared sentinel — set only where two subtests would otherwise be
		// indistinguishable via errors.Is alone (e.g. an empty metadata key
		// vs. one containing a NUL byte both return ErrReverseMapKeyMetadataKey).
		wantErrContains string
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
			key:     MetadataIndexEventKeyV(dal.NewKeyBuilder(), "main", NamespaceAccount, "wallet_id", 1, []byte{0x01}, []byte("users:1"), 1, MetadataEventAdd),
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
			key:             AccountReverseMapKeyV(dal.NewKeyBuilder(), "main", "", "wallet_id", 1),
			wantErr:         ErrReverseMapKeyEntityID,
			wantErrContains: "empty",
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
			// The swallowed real version bytes (0,0,0,1 for version=1) land
			// at the front of the mis-decoded MetadataKey, so the NUL is at
			// offset 0 — distinct from the "metadata key containing a NUL
			// byte" case below, which pins a different offset.
			key:             AccountReverseMapKeyV(dal.NewKeyBuilder(), "main", "us\x00ers", "wallet_id", 1),
			wantErr:         ErrReverseMapKeyMetadataKey,
			wantErrContains: "contains NUL at offset 0",
		},
		"metadata key containing a NUL byte must be rejected directly": {
			// A well-formed account tail (proper terminator, proper 4-byte
			// version) whose metadata-key tail itself contains a raw NUL —
			// isolates the MetadataKey NUL check from the entity-id mis-split
			// cascade covered by the case above. "meta\x00key": the NUL sits
			// at offset 4 within the decoded MetadataKey.
			key: dal.NewKeyBuilder().
				Reset().
				PutByte(PrefixReverseMap).
				PutLedgerNameFixed("main").
				PutNamespace(NamespaceAccount).
				PutStringNull("users:1").
				PutUint32(1).
				PutString("meta\x00key").
				Build(),
			wantErr:         ErrReverseMapKeyMetadataKey,
			wantErrContains: "contains NUL at offset 4",
		},
		"empty metadata key must be rejected for an account entity": {
			key:             AccountReverseMapKeyV(dal.NewKeyBuilder(), "main", "users:1", "", 1),
			wantErr:         ErrReverseMapKeyMetadataKey,
			wantErrContains: "empty",
		},
		"empty metadata key must be rejected for a transaction entity": {
			key:             TransactionReverseMapKeyV(dal.NewKeyBuilder(), "main", 4242, "", 1),
			wantErr:         ErrReverseMapKeyMetadataKey,
			wantErrContains: "empty",
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

			if tt.wantErrContains != "" {
				require.ErrorContains(t, err, tt.wantErrContains, name)
			}
		})
	}
}
