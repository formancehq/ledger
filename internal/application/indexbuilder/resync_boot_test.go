package indexbuilder

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// resyncFixture is a node whose read store folded a DeleteLedger that its main
// store lost in a crash. Both stores commit without fsync, so each keeps its
// own unsynced tail. The node is out of sync and the leader's checkpoint, which
// contains the deletion and one later log, is not yet installed.
type resyncFixture struct {
	b                *Builder
	ledger           string
	folded           uint64
	leaderCheckpoint uint64
}

func newResyncFixture(t *testing.T) resyncFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()
	b.meter = metricnoop.Meter{}
	const ledger = "resync-deleted-ledger"
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
	// The durable main-store image the crashed node reopens with.
	staleMain, err := b.pebbleStore.CreateSnapshot()
	require.NoError(t, err)

	commitBootQueryState(t, b, 3, []*commonpb.Log{{Sequence: 3, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{
		DeleteLedger: &commonpb.DeletedLedgerLog{Name: ledger},
	}}}}, func(batch *dal.WriteSession) {
		require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger, DeletedAt: &commonpb.Timestamp{Data: 3}}))
		require.NoError(t, state.DeleteLedgerData(batch, ledger))
	})
	folded, err := b.processLogs(ctx, 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(3), folded)

	const other = "resync-later-ledger"
	commitBootQueryState(t, b, 4, []*commonpb.Log{{Sequence: 4, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
		CreateLedger: &commonpb.CreatedLedgerLog{Name: other},
	}}}}, func(batch *dal.WriteSession) {
		require.NoError(t, state.SaveLedger(batch, other, &commonpb.LedgerInfo{Name: other}))
	})
	leaderCheckpoint, err := b.pebbleStore.CreateSnapshot()
	require.NoError(t, err)

	require.NoError(t, b.pebbleStore.RestoreCheckpoint(staleMain))
	require.Equal(t, uint64(2), mainLastSequence(t, b))

	return resyncFixture{b: b, ledger: ledger, folded: folded, leaderCheckpoint: leaderCheckpoint}
}

func (f resyncFixture) installLeaderCheckpoint(t *testing.T) {
	t.Helper()
	require.NoError(t, f.b.pebbleStore.RestoreCheckpoint(f.leaderCheckpoint))
	require.Equal(t, uint64(4), mainLastSequence(t, f.b))
}

func mainLastSequence(t *testing.T, b *Builder) uint64 {
	t.Helper()
	handle, err := b.pebbleStore.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()
	seq, err := query.ReadLastSequence(handle)
	require.NoError(t, err)

	return seq
}

func TestBootInitWaitsForMainStoreToReachReadStoreCursor(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	f := newResyncFixture(t)
	b := f.b

	_, _, err := b.bootInit(ctx)
	require.ErrorIs(t, err, errMainStoreBehindReadStore)
	require.NotErrorIs(t, err, errHistoryReplayInvariant, "a lagging main store is retried, not terminal")
	require.Empty(t, b.unresolvedIndexes, "no replay obligation is derived from the lagging main store")

	f.installLeaderCheckpoint(t)
	bootCursor, _, err := b.bootInit(ctx)
	require.NoError(t, err)
	require.Equal(t, f.folded, bootCursor)
	require.Nil(t, b.ledgerConfig(f.ledger))

	caughtUp, err := b.processLogs(ctx, bootCursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(4), caughtUp)
	require.NoError(t, b.validateHistoryReplayState())
}

// The worker starts while follower sync is still fetching the leader's
// checkpoint, as on an out-of-sync restart.
func TestWorkerBootsAfterLeaderSyncRestoresMainStoreBehindReadStore(t *testing.T) {
	t.Parallel()
	f := newResyncFixture(t)
	b := f.b

	b.Start()
	t.Cleanup(b.Stop)

	require.Never(t, func() bool {
		cursor, err := b.readStore.LastIndexedSequence()

		return !b.readStore.ReadProjectionHealthy() || err != nil || cursor != f.folded
	}, 300*time.Millisecond, 10*time.Millisecond, "boot must wait without failing or folding")

	f.installLeaderCheckpoint(t)

	require.Eventually(t, func() bool {
		cursor, err := b.readStore.LastIndexedSequence()

		return err == nil && cursor == 4
	}, 10*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool {
		return !b.readStore.ReadProjectionHealthy()
	}, 300*time.Millisecond, 10*time.Millisecond, "catch-up validation must accept the folded deletion")
}
