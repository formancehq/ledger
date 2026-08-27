package indexbuilder

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func makeAcctSavedMetadataLog(seq uint64, ledger string, ledgerLogID uint64, account, key string, value *commonpb.MetadataValue) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id: ledgerLogID,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SavedMetadata{
								SavedMetadata: &commonpb.SavedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: account}},
									},
									Metadata: map[string]*commonpb.MetadataValue{key: value},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeAcctDeletedMetadataLog(seq uint64, ledger string, ledgerLogID uint64, account, key string) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id: ledgerLogID,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
								DeletedMetadata: &commonpb.DeletedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: account}},
									},
									Key: key,
								},
							},
						},
					},
				},
			},
		},
	}
}

func driveBackfills(t *testing.T, b *Builder, globalCursor uint64) {
	t.Helper()

	b.backfillBudget = time.Second
	stop := make(chan struct{})

	for range 20 {
		if len(b.backfillTasks) == 0 {
			return
		}

		b.processBackfills(context.Background(), stop, globalCursor)
	}

	require.Empty(t, b.backfillTasks, "backfill did not retire within budget")
}

func declareFieldType(t *testing.T, b *Builder, ledger, key string, ft commonpb.MetadataType) {
	t.Helper()

	fsmBatch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SaveLedger(fsmBatch, ledger, &commonpb.LedgerInfo{
		Name: ledger,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				key: {Type: ft},
			},
		},
	}))
	require.NoError(t, fsmBatch.Commit())
	b.seedBatchSchema(t)
}

// liveMembership resolves whether entity is a live member of ANY value group
// of (ledger, key) at the given version and pin — what an exists-style query
// serves off the metadata value index. Over a field-wide scan each resolved
// group is [encodedValue][entity], so the entity is matched as a suffix.
func liveMembership(t *testing.T, b *Builder, ledger, key string, version uint32, entity string, pin uint64) bool {
	t.Helper()

	kb := dal.NewKeyBuilder()
	prefix := append([]byte(nil), readstore.MetadataIndexPrefixV(kb, ledger, readstore.NamespaceAccount, key, version)...)

	iter, err := readstore.NewEventResolveIterator(b.readStore.DB(), prefix, pin)
	require.NoError(t, err)

	defer iter.Close()

	for iter.Next() {
		if bytes.HasSuffix(iter.Current(), []byte(entity)) {
			return true
		}
	}

	require.NoError(t, iter.Err())

	return false
}

// TestDropRecreate_DeletedValueStaysDead replays the recorded phantom-row
// lifecycle end to end:
//
//	write bob.k3 → DropIndex → delete bob.k3 (folds unindexed: skipped)
//	→ retype k3 → CreateIndex again → backfill replays history.
//
// Pre-fix, the recreated index reused version 1, the drop leaked this
// incarnation's rows, and the replay — re-encoding the write under the new
// declared type at its ORIGINAL sequence — emitted a retraction that loses the
// same-sequence tie to the leaked ADD, permanently: the deleted value stayed a
// live member forever. Post-fix the drop purges the rows (A), the recreate
// allocates a fresh version above the tombstone's high-water (B), and the
// promoted index must simply not contain bob.
func TestDropRecreate_DeletedValueStaysDead(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.accounts = make(map[string]struct{})

	const (
		ledger  = "test"
		metaKey = "k3"
		account = "bob"
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)
	canonical := indexes.Canonical(id)
	kb := dal.NewKeyBuilder()

	// Incarnation 1: k3 declared INT64, bob.k3 = 42 in history, index built.
	declareFieldType(t, b, ledger, metaKey, commonpb.MetadataType_METADATA_TYPE_INT64)
	writeLogToFSM(t, b, makeAcctSavedMetadataLog(1, ledger, 1, account, metaKey, commonpb.NewIntValue(42)))
	writeAppliedProposalToFSM(t, b, 1, 1, 1)

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id})
	require.NoError(t, b.wb.Flush())
	driveBackfills(t, b, 1)

	v1, _ := b.versionFor(ledger, canonical)
	require.NotZero(t, v1)

	require.True(t, liveMembership(t, b, ledger, metaKey, v1, account, 99),
		"premise: incarnation 1 serves bob")

	// Drop. Rows purged, version tombstoned.
	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	require.NoError(t, b.handleDroppedIndexLog(b.kb, ledger, &commonpb.DroppedIndexLog{Id: id}))
	require.NoError(t, b.wb.Flush())

	// The delete folds while nothing indexes k3: the live path skips it, and
	// only the eventual replay of this log can account for it.
	writeLogToFSM(t, b, makeAcctDeletedMetadataLog(2, ledger, 2, account, metaKey))
	writeAppliedProposalToFSM(t, b, 2, 2, 2)

	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	require.NoError(t, b.indexPayload(b.kb, b.ledgerConfig(ledger), ledger,
		makeAcctDeletedMetadataLog(2, ledger, 2, account, metaKey).GetPayload().GetApply().GetLog().GetData().GetPayload(), nil))
	require.NoError(t, b.wb.Flush())

	// The retype lands while dropped, so the recreate's replay re-encodes the
	// write at seq 1 under STRING — the encoding divergence that made the
	// same-sequence retraction necessary and impossible.
	declareFieldType(t, b, ledger, metaKey, commonpb.MetadataType_METADATA_TYPE_STRING)

	// Incarnation 2.
	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id})
	require.NoError(t, b.wb.Flush())
	driveBackfills(t, b, 2)

	v2, pending := b.versionFor(ledger, canonical)
	require.Zero(t, pending)
	require.NotZero(t, v2, "re-created index must be READY")
	require.Greater(t, v2, v1, "a version is never reused: the fresh pass gets a keyspace no earlier incarnation wrote")

	// The one assertion the recorded phantom violated: bob's k3 was deleted at
	// seq 2, so no served version may resolve bob as a member at pins past it.
	require.False(t, liveMembership(t, b, ledger, metaKey, v2, account, 99),
		"the deleted value must be dead at the served version")
	require.False(t, liveMembership(t, b, ledger, metaKey, v1, account, 99),
		"the dropped incarnation's keyspace was purged with it")

	// And the purge left nothing behind at all in v1.
	v1prefix := readstore.MetadataIndexPrefixV(kb, ledger, readstore.NamespaceAccount, metaKey, v1)
	iter, err := b.readStore.DB().NewIter(&pebble.IterOptions{
		LowerBound: v1prefix,
		UpperBound: readstore.IncrementBytes(v1prefix),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	require.False(t, iter.First(), "DropIndex purges the incarnation's rows in the same fold batch")
}
