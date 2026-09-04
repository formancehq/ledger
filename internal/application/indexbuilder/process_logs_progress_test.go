package indexbuilder

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func seedLogTarget(t *testing.T, b *Builder, appliedIndex uint64, count int) {
	t.Helper()

	logs := make([]*commonpb.Log, 0, count)
	for sequence := 1; sequence <= count; sequence++ {
		logs = append(logs, &commonpb.Log{Sequence: uint64(sequence)})
	}

	batch := b.pebbleStore.OpenWriteSession()
	if len(logs) > 0 {
		require.NoError(t, state.AppendLogs(batch, logs))
	}
	require.NoError(t, state.SetAppliedIndex(batch, appliedIndex))
	require.NoError(t, batch.Commit())
}

func TestProcessLogsCertifiesAppliedEntryWithoutLogMovement(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 1
	seedLogTarget(t, b, 9, 0)

	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Zero(t, cursor)

	progress, err := b.readStore.ReadRaftProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(9), progress,
		"a failed, no-op, or technical-only Raft entry must still advance the projection certificate")
}

func TestProcessLogsRejectsCorruptTargetLog(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	batch := b.pebbleStore.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(1).Build()
	require.NoError(t, batch.SetBytes(key, []byte{0xff}))
	require.NoError(t, state.SetAppliedIndex(batch, 9))
	require.NoError(t, batch.Commit())

	_, err := b.processLogs(context.Background(), 0, time.Time{})
	require.ErrorContains(t, err, "reading target log sequence")
}

func TestProcessLogsPublishesRaftHorizonOnlyAfterFinalNativeBatch(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 1
	seedLogTarget(t, b, 23, 3)

	// An expired yield deadline makes each call stop after one batch. This
	// models a crash/restart boundary without timing or sleeps.
	cursor, err := b.processLogs(context.Background(), 0, time.Unix(1, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(1), cursor)
	native, err := b.readStore.LastIndexedSequence()
	require.NoError(t, err)
	require.Equal(t, cursor, native)
	progress, err := b.readStore.ReadRaftProgress()
	require.NoError(t, err)
	require.Zero(t, progress, "an intermediate native batch must not certify the fixed target")

	// Advance the main store after the target was captured. Continuation calls
	// must finish the original (H=23, seq=3) target rather than chase this newer
	// head; the next target is captured only after H=23 is published.
	batch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{Sequence: 4}}))
	require.NoError(t, state.SetAppliedIndex(batch, 24))
	require.NoError(t, batch.Commit())

	// Resume from the durable native cursor, as a restarted builder does.
	cursor, err = b.processLogs(context.Background(), native, time.Unix(1, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	progress, err = b.readStore.ReadRaftProgress()
	require.NoError(t, err)
	require.Zero(t, progress, "a crash between batches must leave the target unpublished")

	cursor, err = b.processLogs(context.Background(), cursor, time.Unix(1, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(3), cursor)
	progress, err = b.readStore.ReadRaftProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(23), progress,
		"only the target-completing batch may atomically publish the Raft horizon")

	cursor, err = b.processLogs(context.Background(), cursor, time.Unix(1, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(4), cursor)
	progress, err = b.readStore.ReadRaftProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(24), progress,
		"a later call captures and certifies the head that arrived after the fixed target")
}

func TestProcessLogsWaitsForAuditBeforeFreezingQueryCheckpoint(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 1

	const (
		checkpointID = uint64(41)
		horizon      = uint64(31)
	)
	batch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
			CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
				CheckpointId: checkpointID,
				MaxSequence:  1,
				AppliedIndex: horizon,
			},
		}},
	}}))
	require.NoError(t, state.SetAppliedIndex(batch, horizon))
	require.NoError(t, batch.Commit())

	type result struct {
		cursor uint64
		err    error
	}
	done := make(chan result, 1)
	go func() {
		cursor, err := b.processLogs(context.Background(), 0, time.Time{})
		done <- result{cursor: cursor, err: err}
	}()

	require.Eventually(t, func() bool {
		progress, err := b.readStore.ReadRaftProgress()

		return err == nil && progress == horizon
	}, 5*time.Second, 10*time.Millisecond,
		"the normal projection must reach the checkpoint log before waiting for audit")
	require.False(t, readstore.CheckpointDirReady(b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)),
		"the checkpoint must not be exposed while audit is behind")

	auditBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteAuditRaftProgress(auditBatch, horizon))
	require.NoError(t, auditBatch.Commit())
	b.readStore.NotifyProgress()

	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Equal(t, uint64(1), got.cursor)
	case <-time.After(5 * time.Second):
		t.Fatal("processLogs did not resume after audit reached the fixed checkpoint horizon")
	}

	dir := b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)
	require.True(t, readstore.CheckpointDirReady(dir))
	frozen, err := readstore.OpenReadOnly(dir, noopLogger{})
	require.NoError(t, err)
	defer func() { _ = frozen.Close() }()
	frozenAuditProgress, err := frozen.ReadAuditRaftProgress()
	require.NoError(t, err)
	require.Equal(t, horizon, frozenAuditProgress,
		"the ready checkpoint must contain the audit certificate it promised")
}

func TestProcessLogsAbandonsCheckpointWhenAuditBecomesUnavailable(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 1

	const (
		checkpointID = uint64(42)
		horizon      = uint64(32)
	)
	batch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
			CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
				CheckpointId: checkpointID,
				MaxSequence:  1,
				AppliedIndex: horizon,
			},
		}},
	}}))
	require.NoError(t, state.SetAppliedIndex(batch, horizon))
	require.NoError(t, batch.Commit())

	type result struct {
		cursor uint64
		err    error
	}
	done := make(chan result, 1)
	go func() {
		cursor, err := b.processLogs(context.Background(), 0, time.Time{})
		done <- result{cursor: cursor, err: err}
	}()

	require.Eventually(t, func() bool {
		progress, err := b.readStore.ReadRaftProgress()

		return err == nil && progress == horizon
	}, 5*time.Second, 10*time.Millisecond)
	b.readStore.SetAuditProjectionState(false, true)

	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Equal(t, uint64(1), got.cursor)
	case <-time.After(5 * time.Second):
		t.Fatal("processLogs remained blocked after the audit projection entered rebuild")
	}
	require.False(t, readstore.CheckpointDirReady(b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)),
		"an unavailable audit projection must leave the checkpoint unmaterialized")
}

func TestProcessLogsLeavesCheckpointUnavailableWhenAuditIsNotReady(t *testing.T) {
	t.Parallel()

	for name, projectionState := range map[string]struct {
		disabled   bool
		rebuilding bool
	}{
		"disabled":   {disabled: true},
		"rebuilding": {rebuilding: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			b.notifications = signal.NewNotifications()
			b.batchSize = 1
			b.readStore.SetAuditProjectionState(projectionState.disabled, projectionState.rebuilding)

			const (
				checkpointID = uint64(43)
				horizon      = uint64(33)
			)
			batch := b.pebbleStore.OpenWriteSession()
			require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
				Sequence: 1,
				Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
					CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
						CheckpointId: checkpointID,
						MaxSequence:  1,
						AppliedIndex: horizon,
					},
				}},
			}}))
			require.NoError(t, state.SetAppliedIndex(batch, horizon))
			require.NoError(t, batch.Commit())

			cursor, err := b.processLogs(context.Background(), 0, time.Time{})
			require.NoError(t, err)
			require.Equal(t, uint64(1), cursor)
			require.False(t, readstore.CheckpointDirReady(b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)))
		})
	}
}

func TestProcessLogsCertifiesCheckpointHorizonBeforeLaterTarget(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 1

	const (
		checkpointID      = uint64(44)
		checkpointHorizon = uint64(34)
		targetHorizon     = uint64(35)
	)
	batch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{
		{
			Sequence: 1,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
				CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
					CheckpointId: checkpointID,
					MaxSequence:  1,
					AppliedIndex: checkpointHorizon,
				},
			}},
		},
		{Sequence: 2},
	}))
	require.NoError(t, state.SetAppliedIndex(batch, targetHorizon))
	require.NoError(t, batch.Commit())

	auditBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteAuditRaftProgress(auditBatch, checkpointHorizon))
	require.NoError(t, auditBatch.Commit())

	cursor, err := b.processLogs(context.Background(), 0, time.Unix(1, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(1), cursor)
	progress, err := b.readStore.ReadRaftProgress()
	require.NoError(t, err)
	require.Equal(t, checkpointHorizon, progress,
		"the checkpoint log may certify its own horizon without certifying the later fixed target")
	require.True(t, readstore.CheckpointDirReady(b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)))

	cursor, err = b.processLogs(context.Background(), cursor, time.Unix(1, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	progress, err = b.readStore.ReadRaftProgress()
	require.NoError(t, err)
	require.Equal(t, targetHorizon, progress)
}
