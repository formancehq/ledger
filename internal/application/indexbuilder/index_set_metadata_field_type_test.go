package indexbuilder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// makeSetMetadataFieldTypeLog builds a Log wrapping a SetMetadataFieldType
// payload, the shape indexLogEntry dispatches to indexSetMetadataFieldType.
func makeSetMetadataFieldTypeLog(seq uint64, ledger string, targetType commonpb.TargetType, key string, toType commonpb.MetadataType) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id: 1,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
								SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
									TargetType: targetType,
									Key:        key,
									Type:       toType,
								},
							},
						},
					},
				},
			},
		},
	}
}

// TestIndexSetMetadataFieldType_ReplaysReverseMapRewrite drives a
// SetMetadataFieldType log through indexLogEntry (the backfill-replay path —
// see process_logs.go:546-557 and backfill.go's processBackfill) and asserts
// the reverse-map row and forward index for the affected entity are
// re-encoded under the new type. This is the path that would have caught the
// entityID aliasing bug (a stale sub-slice of a reused Pebble iterator key
// buffer) had it corrupted the account bytes used to key the rewritten
// forward-index entry.
func TestIndexSetMetadataFieldType_ReplaysReverseMapRewrite(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		acct   = "users:alice"
		key    = "role"
	)
	ns := readstore.NamespaceAccount

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	// Seed a committed rmap row + forward index entry at the original
	// (int64) encoding.
	b.wb.Init(b.readStore.NewBatch())
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct, key, 42)))
	require.NoError(t, b.wb.Flush())

	oldEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(42))
	require.True(t, readStoreKeyExists(t, b.readStore, readstore.MetadataIndexKey(
		dal.NewKeyBuilder(), ledger, ns, key, oldEncoded, []byte(acct),
	)), "precondition: old forward-index entry must exist before the rewrite")

	// Replay a SetMetadataFieldType log converting role: INT64 -> STRING.
	log := makeSetMetadataFieldTypeLog(1, ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, commonpb.MetadataType_METADATA_TYPE_STRING)

	b.wb.Init(b.readStore.NewBatch())
	proposals := newTestAppliedProposalSync()
	require.NoError(t, b.indexLogEntry(cfg, log, proposals))
	require.NoError(t, b.wb.Flush())

	// The reverse-map row now holds the string-converted value.
	rmapKey := readstore.AccountReverseMapKeyV(dal.NewKeyBuilder(), ledger, acct, key, 1)
	gotEncoded, err := b.reverseMapValue(rmapKey)
	require.NoError(t, err)
	require.NotNil(t, gotEncoded)

	decoded, _, err := readstore.DecodeValue(gotEncoded)
	require.NoError(t, err)

	wantValue := commonpb.ConvertMetadataValue(commonpb.NewIntValue(42), commonpb.MetadataType_METADATA_TYPE_STRING)
	assert.Equal(t, wantValue.GetStringValue(), decoded.GetStringValue())

	// The old (int) forward-index entry is gone; the new (string) one exists
	// keyed under the correct account — proving entityID survived the
	// rewrite intact.
	newEncoded := readstore.EncodeMetadataValue(nil, wantValue)
	assert.False(t, readStoreKeyExists(t, b.readStore, readstore.MetadataIndexKey(
		dal.NewKeyBuilder(), ledger, ns, key, oldEncoded, []byte(acct),
	)), "old forward-index entry must be deleted by the rewrite")
	assert.True(t, readStoreKeyExists(t, b.readStore, readstore.MetadataIndexKey(
		dal.NewKeyBuilder(), ledger, ns, key, newEncoded, []byte(acct),
	)), "new forward-index entry must exist under the original account")
}

// TestIndexSetMetadataFieldType_PreservesEntityIDAcrossManyRows seeds enough
// rmap rows that the scanning iterator in indexSetMetadataFieldType moves
// several times before the collected entries are consumed (after the scan
// loop closes). Pebble only guarantees iter.Key() is valid until the next
// iterator move, so a caller that stashes a sub-slice of it (rather than a
// clone) risks reading memory the iterator has since reused for a later key.
// This test asserts every entity's rewritten forward-index entry lands under
// its own, correct account — the aliasing bug this migration fixed would
// have shown up here as entries keyed under the wrong (or corrupted) account.
//
// The authoritative guarantee that ParseReverseMapKey's EntityID is
// independent of the source key is readstore/keys_test.go's
// TestParseReverseMapKey_EntityIDIsIndependentOfSourceKey, which asserts the
// clone directly against the source bytes and so cannot rot silently even if
// Pebble ever stopped reusing its key buffer. This test is integration-level
// defence on top of that unit pin: it proves the clone is actually load-
// bearing at this specific call site, end to end through indexLogEntry.
// numAcc = 12 is not tied to any verified Pebble internal buffer or block
// size — the number itself isn't load-bearing, only that it's comfortably
// more than one, so the scan loop is guaranteed to move the iterator several
// times before entries is consumed. Reverting the EntityID clone to a raw
// sub-slice of iter.Key() and re-running this test was checked during
// development: it failed on all but the last-scanned account, confirming
// the sensitivity this comment claims.
func TestIndexSetMetadataFieldType_PreservesEntityIDAcrossManyRows(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "role"
		numAcc = 12
	)
	ns := readstore.NamespaceAccount

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	accounts := make([]string, numAcc)

	b.wb.Init(b.readStore.NewBatch())
	for i := range numAcc {
		acct := fmt.Sprintf("users:acct-%02d", i)
		accounts[i] = acct
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct, key, int64(i))))
	}
	require.NoError(t, b.wb.Flush())

	log := makeSetMetadataFieldTypeLog(1, ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, commonpb.MetadataType_METADATA_TYPE_STRING)

	b.wb.Init(b.readStore.NewBatch())
	proposals := newTestAppliedProposalSync()
	require.NoError(t, b.indexLogEntry(cfg, log, proposals))
	require.NoError(t, b.wb.Flush())

	for i, acct := range accounts {
		rmapKey := readstore.AccountReverseMapKeyV(dal.NewKeyBuilder(), ledger, acct, key, 1)
		gotEncoded, err := b.reverseMapValue(rmapKey)
		require.NoErrorf(t, err, "account %s", acct)
		require.NotNilf(t, gotEncoded, "account %s: rmap row missing after rewrite", acct)

		decoded, _, err := readstore.DecodeValue(gotEncoded)
		require.NoErrorf(t, err, "account %s", acct)

		wantValue := commonpb.ConvertMetadataValue(commonpb.NewIntValue(int64(i)), commonpb.MetadataType_METADATA_TYPE_STRING)
		assert.Equalf(t, wantValue.GetStringValue(), decoded.GetStringValue(), "account %s: rmap value corrupted or misattributed", acct)

		newEncoded := readstore.EncodeMetadataValue(nil, wantValue)
		assert.Truef(t, readStoreKeyExists(t, b.readStore, readstore.MetadataIndexKey(
			dal.NewKeyBuilder(), ledger, ns, key, newEncoded, []byte(acct),
		)), "account %s: forward-index entry missing under its own entityID", acct)
	}
}

// TestIndexSetMetadataFieldType_CorruptReverseMapKeyIsLoud writes a
// malformed reverse-map row directly (bypassing the normal write helpers, so
// it can never be produced by correct code) and asserts that replaying a
// SetMetadataFieldType log over it surfaces a loud error instead of silently
// skipping the corrupt row — the invariant #7 behaviour this migration
// introduced at this site.
func TestIndexSetMetadataFieldType_CorruptReverseMapKeyIsLoud(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "role"
	)
	ns := readstore.NamespaceAccount

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	// Craft a row inside the exact (ledger, ns) rmap scan range that
	// ParseReverseMapKey cannot decode: an account-namespace row missing
	// its 0x00 terminator after the account address.
	prefix := readstore.ReverseMapPrefix(dal.NewKeyBuilder(), ledger, ns)
	corruptKey := append(append([]byte{}, prefix...), []byte("no-null-terminator-here")...)

	seedBatch := b.readStore.NewBatch()
	require.NoError(t, seedBatch.SetBytes(corruptKey, readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(1))))
	require.NoError(t, seedBatch.Commit())

	log := makeSetMetadataFieldTypeLog(1, ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, commonpb.MetadataType_METADATA_TYPE_STRING)

	b.wb.Init(b.readStore.NewBatch())
	proposals := newTestAppliedProposalSync()

	// indexLogEntry's SetMetadataFieldType case cancels and resets the fresh
	// batch it rebinds b.wb to on its own error paths, so nothing is left
	// open here for this test to clean up.
	err := b.indexLogEntry(cfg, log, proposals)
	require.ErrorIs(t, err, readstore.ErrReverseMapKeyTruncated,
		"a corrupt rmap row must fail the replay with the specific decode-failure sentinel, not be silently skipped")
}
