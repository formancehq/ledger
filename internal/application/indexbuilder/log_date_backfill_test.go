package indexbuilder

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// The log date index answers date filters over the same universe ListLogs
// scans, so it holds a row for every log of its ledger — config-mutation logs
// included, on both the live path and the backfill replaying a newly created
// index's history (EN-1987).

func logDateConfig() *ledgerIndexConfig {
	cfg := newLedgerIndexConfig()
	id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	return cfg
}

// makeSchemaLog builds a config-mutation log: an Apply log carrying a ledger
// log whose payload declares a metadata field type. isDataLog rejects it, and
// its date belongs in the log date index all the same.
func makeSchemaLog(seq uint64, ledger string, logID, date uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id:   logID,
						Date: &commonpb.Timestamp{Data: date},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
								SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
									TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
									Key:        "tier",
									Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
								},
							},
						},
					},
				},
			},
		},
	}
}

// scanLogDates returns the (date, logID) pairs the ledger's log date index
// holds, in key order.
func scanLogDates(t *testing.T, store *readstore.Store, ledger string) [][2]uint64 {
	t.Helper()

	prefix := readstore.LedgerLogDateRangePrefix(dal.NewKeyBuilder(), ledger)
	iter, err := store.DB().NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: readstore.IncrementBytes(prefix),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	var out [][2]uint64

	for iter.First(); iter.Valid(); iter.Next() {
		suffix := iter.Key()[len(prefix):]
		require.Len(t, suffix, 16, "key layout: [prefix][timestamp 8B][logID 8B]")
		out = append(out, [2]uint64{binary.BigEndian.Uint64(suffix[:8]), binary.BigEndian.Uint64(suffix[8:])})
	}

	require.NoError(t, iter.Error())

	return out
}

// backfillLogDateRow writes for any log of the ledger, and only when the task
// builds the date index.
func TestBackfillLogDateRow(t *testing.T) {
	t.Parallel()

	const ledger = "test"

	cases := []struct {
		name string
		cfg  *ledgerIndexConfig
		log  *commonpb.Log
		want [][2]uint64
	}{
		{
			name: "config-mutation log of a date task",
			cfg:  logDateConfig(),
			log:  makeSchemaLog(1, ledger, 7, 4242),
			want: [][2]uint64{{4242, 7}},
		},
		{
			name: "data log of a date task",
			cfg:  logDateConfig(),
			log:  makeSchemaLog(1, ledger, 8, 4243),
			want: [][2]uint64{{4243, 8}},
		},
		{
			name: "task that does not build the date index",
			cfg:  acctAssetConfig(),
			log:  makeSchemaLog(1, ledger, 7, 4242),
		},
		{
			name: "log with no Apply payload",
			cfg:  logDateConfig(),
			log:  makeDeleteLedgerLog(1, ledger),
		},
		{
			name: "Apply log with no ledger log",
			cfg:  logDateConfig(),
			log: &commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: ledger}},
			}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)

			batch := b.readStore.NewBatch()
			b.initBatch(batch)
			require.NoError(t, b.backfillLogDateRow(tc.cfg, tc.log))
			require.NoError(t, b.wb.Flush())

			assert.Equal(t, tc.want, scanLogDates(t, b.readStore, ledger))
		})
	}
}

// A log date index declared at ledger birth has no entity history, but the
// ledger's config-mutation logs — the CreateIndex log among them — still need
// their dates, so it backfills instead of being promoted live immediately.
func TestHandleCreatedIndexLog_InitialLogDateBackfills(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	const ledger = "test"

	id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	canonical := indexes.Canonical(id)

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id, Initial: true}))
	require.NoError(t, b.wb.Flush())

	require.Len(t, b.backfillTasks, 1, "an initial log date index must schedule a backfill")

	current, pending := b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(0), current, "not servable until the backfill completes")
	assert.Equal(t, uint32(1), pending)
}

// An initial index's ledger was created in the proposal being folded, so this
// run has seen the ledger's first log and the replay starts there rather than
// walking the whole global log.
func TestHandleCreatedIndexLog_InitialLogDateBackfillsFromLedgerFirstLog(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	const ledger = "test"

	b.ledgerFirstSeq[ledger] = 42

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{
		Id:      indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
		Initial: true,
	}))
	require.NoError(t, b.wb.Flush())

	require.Len(t, b.backfillTasks, 1)
	assert.Equal(t, uint64(41), b.backfillTasks[0].cursor,
		"the replay starts just below the ledger's first log, so it covers every one of them")
}

// The bound comes from the ledger's creation log alone. A restart between that
// creation and the CreateIndex log leaves no entry, and the replay must then
// cover the whole log: a later config log's sequence would start the replay
// after config logs whose dates nothing else will ever write.
func TestRecordLedgerCreation(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	b.recordLedgerCreation("test", 7)
	assert.Equal(t, uint64(7), b.ledgerFirstSeq["test"])

	b.recordLedgerCreation("test", 9)
	assert.Equal(t, uint64(7), b.ledgerFirstSeq["test"], "a re-folded creation log must not move the bound")

	b.recordLedgerCreation("", 3)
	b.recordLedgerCreation("zero", 0)
	assert.NotContains(t, b.ledgerFirstSeq, "")
	assert.NotContains(t, b.ledgerFirstSeq, "zero")
}

// Without a recorded first log — a ledger whose creation this process never
// folded — the replay falls back to the whole log rather than risk skipping
// history.
func TestHandleCreatedIndexLog_InitialLogDateWithoutFirstLogScansFromZero(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	require.NoError(t, b.handleCreatedIndexLog("test", &commonpb.CreatedIndexLog{
		Id:      indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
		Initial: true,
	}))
	require.NoError(t, b.wb.Flush())

	require.Len(t, b.backfillTasks, 1)
	assert.Zero(t, b.backfillTasks[0].cursor)
}

// The replay writes a date row for every log of its ledger, so a history of
// config-mutation logs is fully indexed once the backfill completes.
func TestLogDateBackfillIndexesConfigMutationLogs(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	const ledger = "test"

	writeLogToFSM(t, b, makeSchemaLog(1, ledger, 1, 1001))

	// makeCreatedTxLog stamps neither id nor date; every real ledger log
	// carries both (processing.assignLogIDAndDate), and the log cursor reuses
	// its decoded message, so an unstamped fixture would read back the
	// previous log's date.
	txLog := makeCreatedTxLog(2, ledger, 100, []*commonpb.Posting{
		{Source: "accounts:alice", Destination: "accounts:bob", Asset: "USD/2"},
	})
	txLog.GetPayload().GetApply().GetLog().Id = 2
	txLog.GetPayload().GetApply().GetLog().Date = &commonpb.Timestamp{Data: 1002}
	writeLogToFSM(t, b, txLog)

	writeLogToFSM(t, b, makeSchemaLog(3, ledger, 3, 1003))

	globalCursor, err := query.ReadLastSequence(mustReadHandle(t, b))
	require.NoError(t, err)
	require.Equal(t, uint64(3), globalCursor)

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{
		Id: indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
	}))
	require.NoError(t, b.wb.Flush())
	require.Len(t, b.backfillTasks, 1)

	drainBackfills(t, b, globalCursor)

	assert.Equal(t, [][2]uint64{{1001, 1}, {1002, 2}, {1003, 3}}, scanLogDates(t, b.readStore, ledger),
		"every log of the ledger is indexed, config mutations included")
}

// drainBackfills runs the backfill loop until every task retires.
func drainBackfills(t *testing.T, b *Builder, globalCursor uint64) {
	t.Helper()

	b.backfillBudget = time.Second
	stop := make(chan struct{})

	for range 10 {
		if len(b.backfillTasks) == 0 {
			break
		}

		b.processBackfills(context.Background(), stop, globalCursor)
	}

	require.Empty(t, b.backfillTasks, "backfill task must retire after catch-up")
}
