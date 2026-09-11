package indexbuilder

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func createCheckpointLog(sequence, checkpointID, appliedIndex uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
			CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
				CheckpointId: checkpointID,
				MaxSequence:  sequence - 1,
				AppliedIndex: appliedIndex,
			},
		}},
	}
}

func deleteCheckpointLog(sequence, checkpointID uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedQueryCheckpoint{
			DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{CheckpointId: checkpointID},
		}},
	}
}

// seedCheckpointScenario writes the given logs at applied index appliedIndex
// and registers every checkpoint created by them.
func seedCheckpointScenario(t *testing.T, b *Builder, appliedIndex uint64, logs ...*commonpb.Log) {
	t.Helper()

	batch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, logs))
	require.NoError(t, state.SetAppliedIndex(batch, appliedIndex))
	require.NoError(t, batch.Commit())

	for _, log := range logs {
		if cqc, ok := log.GetPayload().GetType().(*commonpb.LogPayload_CreatedQueryCheckpoint); ok {
			seedQueryCheckpointState(t, b, cqc.CreatedQueryCheckpoint.GetCheckpointId(), appliedIndex, false)
		}
	}
}

func certifyAudit(t *testing.T, b *Builder, appliedIndex uint64) {
	t.Helper()

	batch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteAuditRaftProgress(batch, appliedIndex))
	require.NoError(t, batch.Commit())
	b.readStore.NotifyProgress()
}

func durableCursor(t *testing.T, b *Builder) uint64 {
	t.Helper()

	cursor, err := b.readStore.LastIndexedSequence()
	require.NoError(t, err)

	return cursor
}

// A builder that dies while the checkpoint log is being materialized (here:
// parked on the audit wait) has not committed its cursor past that log. The
// restarted builder resumes at the log, re-indexes nothing and materializes.
func TestProcessLogsResumesAtCheckpointLogAfterDyingDuringMaterialization(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 1

	const (
		checkpointID = uint64(51)
		horizon      = uint64(31)
	)
	seedCheckpointScenario(t, b, horizon, &commonpb.Log{Sequence: 1}, createCheckpointLog(2, checkpointID, horizon))

	type result struct {
		cursor uint64
		err    error
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan result, 1)
	go func() {
		cursor, err := b.processLogs(ctx, 0, time.Time{})
		done <- result{cursor: cursor, err: err}
	}()
	// A parked builder holds a store read handle; release it before the
	// stores close, so a failed assertion reports instead of deadlocking.
	t.Cleanup(func() {
		cancel()
		<-done
		done <- result{}
	})

	require.Eventually(t, func() bool {
		progress, err := b.readStore.ReadRaftProgress()

		return err == nil && progress == horizon
	}, 5*time.Second, 10*time.Millisecond, "the batch crossing the checkpoint log must publish the certificate before waiting for audit")
	// Log 1 writes no rows, so its batch persisted no progress either: the
	// durable cursor is anywhere before the checkpoint log, never at it.
	require.Less(t, durableCursor(t, b), uint64(2), "the cursor must not pass the checkpoint log before the read index is marked")
	readIndexDir := b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)
	require.False(t, dal.CheckpointDirReady(readIndexDir))

	cancel()
	var got result
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processLogs did not stop on cancellation")
	}
	done <- got
	require.Error(t, got.err)
	require.Less(t, got.cursor, uint64(2))
	require.Less(t, durableCursor(t, b), uint64(2), "an interrupted materialization leaves the cursor before the checkpoint log")

	certifyAudit(t, b, horizon)

	// Resume from the durable cursor, as a restarted builder does.
	cursor, err := b.processLogs(context.Background(), durableCursor(t, b), time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	require.Equal(t, uint64(2), durableCursor(t, b))
	require.True(t, dal.CheckpointDirReady(readIndexDir))

	frozen, err := readstore.OpenReadOnly(readIndexDir, noopLogger{})
	require.NoError(t, err)
	defer func() { _ = frozen.Close() }()
	frozenProgress, err := frozen.ReadRaftProgress()
	require.NoError(t, err)
	require.Equal(t, horizon, frozenProgress, "the frozen read index must carry the certificate it promised")
}

// A builder that dies after marking the read index but before committing its
// cursor re-crosses the log: the marked directory is kept as is and the cursor
// moves past the log.
func TestProcessLogsRecrossKeepsMarkedReadIndex(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 1

	const (
		checkpointID = uint64(52)
		horizon      = uint64(32)
	)
	seedCheckpointScenario(t, b, horizon, &commonpb.Log{Sequence: 1}, createCheckpointLog(2, checkpointID, horizon))
	certifyAudit(t, b, horizon)

	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	readIndexDir := b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)
	require.True(t, dal.CheckpointDirReady(readIndexDir))

	// The residue of dying between the marker and the cursor commit: the
	// durable cursor still points before the checkpoint log.
	batch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteProgress(batch, 1))
	require.NoError(t, batch.Commit())
	sentinel := filepath.Join(readIndexDir, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, nil, 0o600))

	cursor, err = b.processLogs(context.Background(), 1, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	require.Equal(t, uint64(2), durableCursor(t, b))
	require.FileExists(t, sentinel, "a marked read index must not be rebuilt on the re-cross")
}

// A checkpoint log met mid-batch ends the batch before it and heads the next
// one alone, so the cursor commit that precedes a materialization covers no
// rows the re-cross would have to write again.
func TestProcessLogsGivesCheckpointLogsTheirOwnBatch(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 10

	const (
		createdID = uint64(53)
		deletedID = uint64(54)
		horizon   = uint64(33)
	)
	seedCheckpointScenario(t, b, horizon,
		&commonpb.Log{Sequence: 1},
		&commonpb.Log{Sequence: 2},
		createCheckpointLog(3, createdID, horizon),
		&commonpb.Log{Sequence: 4},
		deleteCheckpointLog(5, deletedID),
		&commonpb.Log{Sequence: 6},
	)
	certifyAudit(t, b, horizon)
	// A stale directory for the deleted checkpoint, to observe the deletion.
	deletedDir := b.pebbleStore.QueryCheckpointReadIndexDir(deletedID)
	require.NoError(t, os.MkdirAll(deletedDir, 0o750))

	// An expired deadline stops each call after one batch.
	expired := time.Unix(1, 0)
	want := []uint64{2, 3, 4, 5, 6}
	cursor := uint64(0)
	for _, wantCursor := range want {
		var err error
		cursor, err = b.processLogs(context.Background(), cursor, expired)
		require.NoError(t, err)
		require.Equal(t, wantCursor, cursor)
		require.Equal(t, wantCursor, durableCursor(t, b))
	}

	require.True(t, dal.CheckpointDirReady(b.pebbleStore.QueryCheckpointReadIndexDir(createdID)))
	require.NoDirExists(t, deletedDir)
}
