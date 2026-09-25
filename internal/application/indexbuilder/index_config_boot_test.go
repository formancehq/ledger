package indexbuilder

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A read pins the main store before DropIndex while the booting builder sees
// the later registry. Its durable config must still fold the earlier transaction
// before certifying a checkpoint. The fixture seeds synthetic committed output;
// it does not replay the original CI history or exercise write admission.
func TestBootInitPreservesAddressQueryPinnedBeforeDrop(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	initialMain, err := dal.NewStore(t.TempDir(), noopLogger{}, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	mainClosed := false
	t.Cleanup(func() {
		if !mainClosed {
			require.NoError(t, initialMain.Close())
		}
	})
	initialRead, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	readClosed := false
	t.Cleanup(func() {
		if !readClosed {
			require.NoError(t, initialRead.Close())
		}
	})
	b := NewBuilder(initialMain, initialRead, attributes.New(), noopLogger{}, noop.NewMeterProvider().Meter("test"), DefaultBatchSize)
	b.notifications = signal.NewNotifications()
	const ledger, account = "boot-address", "t-3:162"
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	canonical := indexes.Canonical(id)
	logs := []*commonpb.Log{
		{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger}}}},
		bootQueryApplyLog(ledger, 2, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: id}}}),
	}
	commitBootQueryState(t, b, 2, logs, func(batch *dal.WriteSession) {
		require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
		_, err := b.attrs.Index.Set(batch, indexes.KeyFor(ledger, id).Bytes(), &commonpb.Index{Id: id, Ledger: ledger, ForwardEncodingVersion: 1})
		require.NoError(t, err)
	})
	before, err := b.processLogs(ctx, 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), before)
	current, pending := b.versionFor(ledger, canonical)
	require.Equal(t, uint32(1), current)
	require.Zero(t, pending)
	require.NoError(t, b.readStore.DB().Flush())

	tx := &commonpb.Transaction{Id: 162, Postings: []*commonpb.Posting{{Source: "world", Destination: account, Asset: "USD", Amount: commonpb.NewUint256FromUint64(1)}}}
	commitBootQueryState(t, b, 3, []*commonpb.Log{
		bootQueryApplyLog(ledger, 3, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: tx}}}),
	}, func(batch *dal.WriteSession) {
		_, err := b.attrs.Transaction.Set(batch, (domain.TransactionKey{LedgerName: ledger, ID: 162}).Bytes(), &commonpb.TransactionState{CreatedByLog: 3})
		require.NoError(t, err)
		_, err = b.attrs.Volume.Set(batch, domain.NewVolumeKey(ledger, account, "USD", "").Bytes(), &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(1)})
		require.NoError(t, err)
	})
	// Reopen both durable stores before starting the request. No in-memory
	// registry or request survives the restart. Path returns the same parent
	// directory that readstore.New expects, containing the readindex database.
	mainDir, readDir := b.pebbleStore.DataDir(), b.readStore.Path()
	require.NoError(t, b.pebbleStore.Close())
	mainClosed = true
	require.NoError(t, b.readStore.Close())
	readClosed = true
	mainStore, err := dal.NewStore(mainDir, b.logger, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mainStore.Close()) })
	readStore, err := readstore.New(readDir, b.logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readStore.Close()) })
	b = NewBuilder(mainStore, readStore, attributes.New(), b.logger, noop.NewMeterProvider().Meter("test"), DefaultBatchSize)
	b.notifications = signal.NewNotifications()
	c := ctrl.NewDefaultController(nil, b.pebbleStore, b.logger, b.attrs, b.readStore, nil, noop.NewMeterProvider().Meter("test"))
	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "t-3:"}}}}
	observed := &bootQueryWaitContext{Context: query.WithReadBarrierHorizon(ctx, 3), waiting: make(chan struct{})}
	type result struct {
		ids []uint64
		err error
	}
	results := make(chan result, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		cur, err := c.ListTransactions(observed, ledger, 3, 111, filter, true)
		if err != nil {
			results <- result{err: err}

			return
		}
		txs, err := cursor.Collect(cur)
		ids := make([]uint64, 0, len(txs))
		for _, tx := range txs {
			ids = append(ids, tx.GetId())
		}
		results <- result{ids: ids, err: err}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-readDone:
		case <-time.After(5 * time.Second):
			t.Error("query did not stop after cancellation")
		}
	})
	select {
	case <-observed.waiting:
	case early := <-results:
		t.Fatalf("read escaped alignment before transaction fold: %+v", early)
	case <-ctx.Done():
		t.Fatal("read did not enter alignment")
	}

	// A checkpoint and a later drop commit before boot reads the registry.
	// The live reader already holds the H=3 main snapshot where ADDRESS exists.
	commitBootQueryState(t, b, 4, []*commonpb.Log{createCheckpointLog(4, 1, 4)}, func(batch *dal.WriteSession) {
		require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: 1, AppliedIndex: 4, MaxSequence: 3}))
	})
	commitBootQueryState(t, b, 5, []*commonpb.Log{
		bootQueryApplyLog(ledger, 5, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DropIndex{DropIndex: &commonpb.DroppedIndexLog{Id: id}}}),
	}, func(batch *dal.WriteSession) {
		require.NoError(t, b.attrs.Index.Delete(batch, indexes.KeyFor(ledger, id).Bytes()))
	})
	bootCursor, _, err := b.bootInit(ctx)
	require.NoError(t, err)
	require.Equal(t, before, bootCursor)

	// Audit deliberately lags the checkpoint. Real processLogs publishes its
	// intermediate certificate and then waits before reaching the later drop.
	b.readStore.SetAuditProjectionState(false, false)
	foldCtx, stopFold := context.WithCancel(ctx)
	foldDone := make(chan error, 1)
	go func() { _, err := b.processLogs(foldCtx, bootCursor, time.Time{}); foldDone <- err }()
	t.Cleanup(func() {
		stopFold()
		select {
		case err := <-foldDone:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("checkpoint wait stopped with %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("index fold did not stop after cancellation")
		}
	})
	require.Eventually(t, func() bool {
		h, err := b.readStore.ReadRaftProgress()

		return err == nil && h == 4
	}, 5*time.Second, time.Millisecond, "checkpoint must publish its certificate before audit catches up")
	indexed, err := b.readStore.LastIndexedSequence()
	require.NoError(t, err)
	require.Equal(t, uint64(3), indexed, "transaction folded, checkpoint action and later drop still pending")
	// A partial audit batch commits its native progress and notifies waiters
	// without certifying its full source horizon, as auditindexer.processBatch
	// does while still behind. The checkpoint and later drop remain pending.
	auditBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteAuditProgress(auditBatch, 1))
	require.NoError(t, auditBatch.Commit())
	auditHorizon, err := b.readStore.ReadAuditRaftProgress()
	require.NoError(t, err)
	require.Zero(t, auditHorizon)
	b.readStore.NotifyProgress()
	select {
	case got := <-results:
		require.NoError(t, got.err)
		require.Equal(t, []uint64{162}, got.ids, "boot must fold historical ADDRESS membership before certifying the checkpoint")
	case <-ctx.Done():
		t.Fatal("live read did not complete at the intermediate certificate")
	}
}

// The real WaitForRaftProgress cancellation watcher calls Done only after
// ListTransactions has pinned main and found an insufficient index certificate.
// No sleep or production hook is needed to pause at that boundary.
type bootQueryWaitContext struct {
	context.Context

	once    sync.Once
	waiting chan struct{}
}

func (c *bootQueryWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })

	return c.Context.Done()
}

// commitBootQueryState installs synthetic committed output atomically,
// including the AppliedProposal coverage required by the real posting fold.
func commitBootQueryState(t *testing.T, b *Builder, horizon uint64, logs []*commonpb.Log, mutate func(*dal.WriteSession)) {
	t.Helper()
	batch := b.pebbleStore.OpenWriteSession()
	if mutate != nil {
		mutate(batch)
	}
	if len(logs) > 0 {
		require.NoError(t, state.AppendLogs(batch, logs))
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAppliedProposal).PutUint64(horizon).Build()
		require.NoError(t, batch.SetProto(key, &proposalpb.AppliedProposal{
			Sequence: horizon, MinLogSequence: logs[0].GetSequence(), MaxLogSequence: logs[len(logs)-1].GetSequence(),
		}))
	}
	require.NoError(t, state.SetAppliedIndex(batch, horizon))
	require.NoError(t, batch.Commit())
}

func bootQueryApplyLog(ledger string, sequence uint64, payload *commonpb.LedgerLogPayload) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: ledger,
			Log:        &commonpb.LedgerLog{Id: sequence - 1, Date: &commonpb.Timestamp{Data: sequence}, Data: payload},
		}}},
	}
}

func TestBootInitRestoresIndexStateWithoutMainRegistry(t *testing.T) {
	t.Parallel()
	indexesToTest := []struct {
		name string
		id   *commonpb.IndexID
	}{
		{"transaction address", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)},
		{"transaction reference", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)},
		{"account asset", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)},
		{"log date", indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)},
		{"account metadata", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")},
		{"transaction metadata", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "role")},
	}
	states := []struct {
		name     string
		version  readstore.IndexVersionState
		backfill bool
		rewrite  bool
	}{
		{name: "current", version: readstore.IndexVersionState{CurrentVersion: 1, HighWater: 1}},
		{name: "pending", version: readstore.IndexVersionState{PendingVersion: 1, HighWater: 1}, backfill: true},
		{name: "tombstone", version: readstore.IndexVersionState{HighWater: 1}},
		{name: "rewrite", version: readstore.IndexVersionState{
			CurrentVersion: 1, PendingVersion: 2, HighWater: 2,
			CurrentType: commonpb.MetadataType_METADATA_TYPE_STRING, CurrentTypeDeclared: true,
			PendingType: commonpb.MetadataType_METADATA_TYPE_INT64, PendingTypeDeclared: true,
		}, rewrite: true},
	}
	for _, index := range indexesToTest {
		for _, test := range states {
			if test.rewrite && index.id.GetMetadata() == nil {
				continue // Schema rewrites exist only for metadata indexes.
			}
			t.Run(index.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				b := newTestBuilderWithStore(t)
				const ledger = "boot-index-state"
				canonical := indexes.Canonical(index.id)
				logs := []*commonpb.Log{
					{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
					}}},
					makeSavedAccountMetadataLog(2, ledger, "seed", "seed", "history"),
					bootQueryApplyLog(ledger, 3, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{
						CreateIndex: &commonpb.CreatedIndexLog{Id: index.id},
					}}),
				}
				if test.rewrite {
					logs = append(logs, bootQueryApplyLog(ledger, 4, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
						SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
							TargetType: index.id.GetMetadata().GetTarget(), Key: "role", Type: commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}}))
				}
				folded := uint64(len(logs))
				dropped := folded + 1
				logs = append(logs, bootQueryApplyLog(ledger, dropped, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DropIndex{
					DropIndex: &commonpb.DroppedIndexLog{Id: index.id},
				}}))
				commitBootQueryState(t, b, dropped, logs, func(batch *dal.WriteSession) {
					require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
				})
				// Seed only the durable local recovery records. The main registry
				// has already applied DropIndex, while active local states precede it.
				if test.version.Tombstoned() {
					folded = dropped
				}
				persistLedgerHistory(t, b, ledger, ledgerHistoryNonEmpty)
				batch := b.readStore.NewBatch()
				require.NoError(t, b.readStore.WriteProgress(batch, folded))
				require.NoError(t, b.readStore.WriteRaftProgress(batch, folded))
				require.NoError(t, b.readStore.WriteIndexVersionState(batch, ledger, canonical, test.version))
				if test.backfill {
					require.NoError(t, b.readStore.WriteBackfillProgress(batch, backfillBBKey(ledger, index.id), 2))
				}
				if test.rewrite {
					encodedCursor := append([]byte{byte(test.version.PendingType)}, []byte("saved-rmap-cursor")...)
					require.NoError(t, b.readStore.WriteBackfillCursor(batch,
						schemaRewriteBBKey(ledger, index.id.GetMetadata().GetTarget(), "role"), encodedCursor))
				}
				require.NoError(t, batch.Commit())

				// Repeated boot attempts must restore one owner per active task.
				for range 2 {
					gotCursor, mainCursor, err := b.bootInit(ctx)
					require.NoError(t, err)
					require.Equal(t, folded, gotCursor)
					require.Equal(t, dropped, mainCursor)
					cached, exists := b.versionStateFor(ledger, canonical)
					require.True(t, exists)
					// The persistent decoder represents an absent opaque tail as
					// an empty slice. Check it before normalizing this representation.
					require.Empty(t, cached.RewriteProgress)
					expected := test.version
					expected.RewriteProgress = []byte{}
					require.Equal(t, expected, cached, "boot preserves version numbers, high-water and type bindings")
					require.Equal(t, !test.version.Tombstoned(), b.ledgerConfig(ledger).isIndexed(index.id))
					if !test.version.Tombstoned() {
						recovered := b.ledgerConfig(ledger).byCanonical[canonical]
						require.Equal(t, ledger, recovered.GetLedger())
						require.True(t, indexes.Equal(index.id, recovered.GetId()))
					}
					if test.backfill {
						require.Len(t, b.backfillTasks, 1)
						require.Equal(t, ledger, b.backfillTasks[0].ledger)
						require.True(t, indexes.Equal(index.id, b.backfillTasks[0].index))
						require.Equal(t, uint64(2), b.backfillTasks[0].cursor)
					} else {
						require.Empty(t, b.backfillTasks)
					}
					if test.rewrite {
						require.Len(t, b.schemaRewriteTasks, 1)
						task := b.schemaRewriteTasks[0]
						require.Equal(t, ledger, task.ledger)
						require.Equal(t, index.id.GetMetadata().GetTarget(), task.targetType)
						require.Equal(t, "role", task.key)
						require.Equal(t, test.version.PendingType, task.toType)
						require.Equal(t, []byte("saved-rmap-cursor"), task.rmapCursor)
					} else {
						require.Empty(t, b.schemaRewriteTasks)
					}
				}
			})
		}
	}
}

func TestBootInitRetainsDeletedLedgerUntilDeleteReplay(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()
	const ledger = "boot-deleted-ledger"
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	commitBootQueryState(t, b, 2, []*commonpb.Log{
		{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
		}}},
		bootQueryApplyLog(ledger, 2, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{
			CreateIndex: &commonpb.CreatedIndexLog{Id: id},
		}}),
	}, func(batch *dal.WriteSession) {
		require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
		_, err := b.attrs.Index.Set(batch, indexes.KeyFor(ledger, id).Bytes(), &commonpb.Index{Ledger: ledger, Id: id, ForwardEncodingVersion: 1})
		require.NoError(t, err)
	})
	before, err := b.processLogs(ctx, 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), before)
	commitBootQueryState(t, b, 3, []*commonpb.Log{{Sequence: 3, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{
		DeleteLedger: &commonpb.DeletedLedgerLog{Name: ledger},
	}}}}, func(batch *dal.WriteSession) {
		require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger, DeletedAt: &commonpb.Timestamp{Data: 3}}))
		require.NoError(t, state.DeleteLedgerData(batch, ledger))
	})
	bootCursor, _, err := b.bootInit(ctx)
	require.NoError(t, err)
	require.Equal(t, before, bootCursor)
	require.True(t, b.ledgerConfig(ledger).isIndexed(id), "main deletion must not erase the historical replay config")
	require.ErrorIs(t, b.validateHistoryReplayState(), errHistoryReplayInvariant,
		"restoring a historical ledger must retain the obligation to consume its DeleteLedger")
	// A cancelled deletion batch must restore its replay obligation as well
	// as its config. The real processLogs retry below must then discharge it.
	aborted := b.readStore.NewBatch()
	b.initFoldBatch(aborted)
	require.NoError(t, b.observeDeletedLedger(ledger))
	require.NoError(t, aborted.Cancel())
	b.rollbackFoldBatch()
	b.wb.Reset()
	require.True(t, b.ledgerConfig(ledger).isIndexed(id))
	require.ErrorIs(t, b.validateHistoryReplayState(), errHistoryReplayInvariant,
		"rolling back DeleteLedger must keep its missing-replay guard armed")
	end, err := b.processLogs(ctx, bootCursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(3), end)
	require.Nil(t, b.ledgerConfig(ledger))
	_, exists := b.historyStateFor(ledger)
	require.False(t, exists)
	require.NoError(t, b.validateHistoryReplayState())
}
