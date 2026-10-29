package indexbuilder

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Repair reuses the supported DropIndex/CreateIndex lifecycle. The old READY
// projection deliberately lacks purge-time rows, as an older builder would.
func TestHistoricalAddressRepairDropRecreate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		index    commonpb.TransactionBuiltinIndex
		prefix   byte
		retained uint64
		want     []uint64
	}{
		{"any", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS, readstore.PrefixAccountTx, 10, []uint64{10, 11, 12}},
		{"source", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS, readstore.PrefixSourceAccountTx, 11, []uint64{11, 12}},
		{"destination", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS, readstore.PrefixDestinationAccountTx, 10, []uint64{10, 12}},
	} {
		for _, route := range []string{"postings", "generic"} {
			t.Run(tc.name+"/"+route, func(t *testing.T) {
				t.Parallel()
				b := newTestBuilderWithStore(t)
				const ledger, address = "repair", "payments:ephemeral"
				id := indexes.TxBuiltinID(tc.index)
				canonical := indexes.Canonical(id)
				seedCachedLedgerHistory(b, ledger, ledgerHistoryNonEmpty)

				fund := makeCreatedTxLog(1, ledger, 10, []*commonpb.Posting{
					{Source: "world", Destination: address, Asset: "USD/2"},
				})
				drain := makeCreatedTxLog(2, ledger, 11, []*commonpb.Posting{
					{Source: address, Destination: "merchant", Asset: "USD/2"},
				})
				drain.GetPayload().GetApply().GetLog().PurgedVolumes = []*commonpb.TouchedVolume{{Account: address, Asset: "USD/2"}}
				netZero := makeCreatedTxLog(3, ledger, 12, []*commonpb.Posting{
					{Source: "world", Destination: address, Asset: "USD/2"},
					{Source: address, Destination: "merchant", Asset: "USD/2"},
				})
				netZero.GetPayload().GetApply().GetLog().EphemeralVolumes = []*commonpb.TouchedVolume{{Account: address, Asset: "USD/2"}}
				for _, log := range []*commonpb.Log{fund, drain, netZero} {
					writeLogToFSM(t, b, log)
					if log.GetSequence() < 3 {
						writeAppliedProposalToFSM(t, b, log.GetSequence(), log.GetSequence(), log.GetSequence())
					}
				}

				// Simulate the deployed READY projection, retaining a valid row while
				// omitting the pure-ephemeral transaction in every role bucket.
				cfg := b.getOrCreateLedgerConfig(ledger)
				cfg.byCanonical[canonical] = &commonpb.Index{Id: id, Ledger: ledger}
				ready := readstore.IndexVersionState{CurrentVersion: 1, HighWater: 1}
				b.putVersionState(ledger, canonical, ready)
				batch := b.readStore.NewBatch()
				require.NoError(t, b.readStore.WriteIndexVersionState(batch, ledger, canonical, ready))
				require.NoError(t, batch.SetBytes(readstore.AccountTxKey(b.kb, tc.prefix, ledger, address, tc.retained), nil))
				require.NoError(t, batch.Commit())
				require.Equal(t, []uint64{tc.retained}, historicalRepairIDs(t, b, tc.prefix, ledger, address))
				frozenDir := filepath.Join(t.TempDir(), "frozen")
				require.NoError(t, b.readStore.CreateCheckpoint(frozenDir))
				frozen, err := readstore.OpenReadOnly(frozenDir, noopLogger{})
				require.NoError(t, err)
				defer func() { require.NoError(t, frozen.Close()) }()

				batch = b.readStore.NewBatch()
				b.initBatch(batch)
				require.NoError(t, b.handleDroppedIndexLog(b.kb, ledger, &commonpb.DroppedIndexLog{Id: id}))
				require.NoError(t, b.wb.Flush())
				current, pending := b.versionFor(ledger, canonical)
				require.Zero(t, current)
				require.Zero(t, pending)
				require.Equal(t, []uint64{tc.retained}, historicalRepairIDs(t, b, tc.prefix, ledger, address), "drop preserves additive address mappings")

				batch = b.readStore.NewBatch()
				b.initBatch(batch)
				require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id}))
				require.NoError(t, b.wb.Flush())
				current, pending = b.versionFor(ledger, canonical)
				require.Zero(t, current, "queries must remain behind readiness while repair runs")
				require.Equal(t, uint32(2), pending)
				require.Len(t, b.backfillTasks, 1)

				// Missing proposal evidence after valid historical rows must reject the
				// whole batch. Neither rows nor its resume cursor may advance, so the
				// repaired evidence can be retried without skipping the missing rows.
				task := b.backfillTasks[0]
				process := b.processBackfillPostings
				if route == "generic" {
					process = b.processBackfill
				}
				err = process(context.Background(), make(chan struct{}), task, time.Now().Add(time.Second))
				require.ErrorContains(t, err, "applied proposal cursor failed")
				require.Zero(t, task.cursor)
				require.Equal(t, []uint64{tc.retained}, historicalRepairIDs(t, b, tc.prefix, ledger, address))
				writeAppliedProposalToFSM(t, b, 3, 3, 3)
				if route == "generic" {
					require.NoError(t, process(context.Background(), make(chan struct{}), task, time.Now().Add(time.Second)))
					require.Equal(t, uint64(3), task.cursor)
					require.NoError(t, b.completeBackfill(task))
				} else {
					driveBackfills(t, b, 3)
				}
				current, pending = b.versionFor(ledger, canonical)
				require.Equal(t, uint32(2), current)
				require.Zero(t, pending)
				require.Equal(t, tc.want, historicalRepairIDs(t, b, tc.prefix, ledger, address))
				require.Equal(t, []uint64{tc.retained}, historicalRepairReaderIDs(t, frozen.DB(), tc.prefix, ledger, address), "live repair must leave the frozen physical copy unchanged")
			})
		}
	}
}

func historicalRepairIDs(t *testing.T, b *Builder, bucket byte, ledger, address string) []uint64 {
	t.Helper()

	return historicalRepairReaderIDs(t, b.readStore.DB(), bucket, ledger, address)
}

func historicalRepairReaderIDs(t *testing.T, reader dal.PebbleReader, bucket byte, ledger, address string) []uint64 {
	t.Helper()
	prefix := readstore.AccountTxPrefix(dal.NewKeyBuilder(), bucket, ledger, address)
	iter, err := readstore.NewPrefixIterator(reader, prefix, len(prefix), 8)
	require.NoError(t, err)
	defer iter.Close()
	var ids []uint64
	for iter.Next() {
		ids = append(ids, binary.BigEndian.Uint64(iter.Current()))
	}
	require.NoError(t, iter.Err())

	return ids
}

// The real live fold commits mappings and progress together even when one
// applied proposal contains a funding/draining pair for a purged account.
func TestHistoricalAddressLiveFoldPreservesPurgedHistory(t *testing.T) {
	t.Parallel()
	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = DefaultBatchSize
	const ledger, address = "live", "payments:ephemeral"
	persistLedgerHistory(t, b, ledger, ledgerHistoryNonEmpty)
	for _, index := range []commonpb.TransactionBuiltinIndex{
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS,
	} {
		id := indexes.TxBuiltinID(index)
		b.getOrCreateLedgerConfig(ledger).byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id, Ledger: ledger}
		b.putVersionState(ledger, indexes.Canonical(id), readstore.IndexVersionState{CurrentVersion: 1, HighWater: 1})
	}
	fund := makeCreatedTxLog(1, ledger, 10, []*commonpb.Posting{{Source: "world", Destination: address, Asset: "USD/2"}})
	drain := makeCreatedTxLog(2, ledger, 11, []*commonpb.Posting{{Source: address, Destination: "merchant", Asset: "USD/2"}})
	drain.GetPayload().GetApply().GetLog().PurgedVolumes = []*commonpb.TouchedVolume{{Account: address, Asset: "USD/2"}}
	writeLogToFSM(t, b, fund)
	writeLogToFSM(t, b, drain)
	writeAppliedProposalToFSM(t, b, 1, 1, 2)
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	require.Equal(t, uint64(2), durableCursor(t, b))
	require.Equal(t, []uint64{10, 11}, historicalRepairIDs(t, b, readstore.PrefixAccountTx, ledger, address))
	require.Equal(t, []uint64{11}, historicalRepairIDs(t, b, readstore.PrefixSourceAccountTx, ledger, address))
	require.Equal(t, []uint64{10}, historicalRepairIDs(t, b, readstore.PrefixDestinationAccountTx, ledger, address))
}
