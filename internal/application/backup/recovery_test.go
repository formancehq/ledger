package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	infrabackup "github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source=../../infra/backup/storage.go -destination=recovery_storage_generated_test.go -package=backup Storage

// recoveryFixture injects one transport failure at the Proposer boundary. It
// executes and commits the real job-state transitions, not Raft elections. The
// orchestration, checkpoint work and cleanup scan remain real.
type recoveryFixture struct {
	t               *testing.T
	store           *dal.Store
	jobs            *state.BackupJobsState
	orchestrator    *Orchestrator
	cleanup         *Cleanup
	storage         *MockStorage
	objects         map[string][]byte
	dst             *raftcmdpb.BackupDestination
	index           uint64
	starts          []uint64
	terminals       []*raftcmdpb.BackupJob
	puts            int
	deletes         int
	failPut         bool
	putFailures     int
	fault           string
	faults          int
	cleanupFails    int
	cleanupAttempts int
	errLost         error
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	f := &recoveryFixture{
		t: t, store: newTempStore(t), jobs: state.NewBackupJobsState(),
		objects: map[string][]byte{}, dst: testDestination(),
		errLost: errors.New("injected proposal response unavailable"),
	}
	ctrl := gomock.NewController(t)
	proposer := NewMockProposer(ctrl)
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(f.propose)
	f.orchestrator = newOrchestrator(proposer, f.store)
	t.Cleanup(func() { f.orchestrator.OnLeadershipChange(false) })
	leader := NewMockLeaderProbe(ctrl)
	leader.EXPECT().IsLeader().AnyTimes().Return(true)
	f.cleanup = NewCleanup(f.jobs, proposer, leader, f.orchestrator.Registry(), logging.Testing())
	f.storage = NewMockStorage(ctrl)
	f.storage.EXPECT().PutFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, key string, data io.Reader, _ int64) error {
			// Read uploaded checkpoint files even when injecting failure.
			body, err := io.ReadAll(data)
			if err != nil {
				return err
			}
			f.puts++
			if f.failPut {
				f.failPut = false
				f.putFailures++

				return context.Canceled
			}
			f.objects[key] = body

			return nil
		})
	f.storage.EXPECT().GetFile(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, key string) (io.ReadCloser, error) {
			body, ok := f.objects[key]
			if !ok {
				return nil, infrabackup.ErrFileNotFound
			}

			return io.NopCloser(bytes.NewReader(body)), nil
		})
	f.storage.EXPECT().ListFiles(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, prefix string) ([]string, error) {
			var keys []string
			for key := range f.objects {
				if strings.HasPrefix(key, prefix) {
					keys = append(keys, key)
				}
			}

			return keys, nil
		})
	f.storage.EXPECT().DeleteFile(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, key string) error {
			f.deletes++
			delete(f.objects, key)

			return nil
		})

	return f
}

func (f *recoveryFixture) propose(_ context.Context, proposal *raftcmdpb.Proposal) error {
	var start *raftcmdpb.BackupOrderStart
	var complete *raftcmdpb.BackupOrderComplete
	var fail *raftcmdpb.BackupOrderFail
	kind := raftcmdpb.BackupKind_BACKUP_KIND_FULL
	order := extractFullOrder(proposal)
	require.NotNil(f.t, order)
	start, complete, fail = order.GetStart(), order.GetComplete(), order.GetFail()
	isCleanup := fail != nil && fail.GetMessage() == "orphan: no live executor on this leader"
	if isCleanup {
		f.cleanupAttempts++
		if f.cleanupFails > 0 {
			f.cleanupFails--
			f.t.Logf("cleanup Fail job=%d dropped before apply", fail.GetJobId())

			return f.errLost
		}
	}
	if f.faults == 0 && ((f.fault == "lost-fail" && fail != nil && !isCleanup) ||
		(f.fault == "lost-complete" && complete != nil)) {
		f.faults++
		f.t.Logf("%s dropped before apply", f.fault)

		return f.errLost
	}
	batch := f.store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }() // Safe after Commit; no pending writes remain.
	f.index++
	var job *raftcmdpb.BackupJob
	var err error
	switch {
	case start != nil:
		f.starts = append(f.starts, start.GetJobId())
		job, _, err = f.jobs.Start(batch, f.index, kind, start)
	case complete != nil:
		job, err = f.jobs.Complete(batch, f.index, complete)
	case fail != nil:
		job, err = f.jobs.Fail(batch, f.index, fail)
	default:
		f.t.Fatal("proposal has no lifecycle operation")
	}
	if err != nil {
		return err
	}
	require.NoError(f.t, batch.Commit())
	if start != nil {
		f.t.Logf("Start committed job=%d kind=%s", job.GetJobId(), kind)
		// The real registry must fence cleanup throughout Start apply, before
		// any upload. A cleanup tick here must not emit a Fail proposal.
		before := f.cleanupAttempts
		f.cleanup.tick(context.Background())
		require.Equal(f.t, before, f.cleanupAttempts, "live executor must not be orphaned")
	} else {
		f.terminals = append(f.terminals, job.CloneVT())
		f.t.Logf("terminal committed job=%d status=%s", job.GetJobId(), job.GetStatus())
	}
	if f.faults == 0 && ((f.fault == "start-ack" && start != nil) ||
		(f.fault == "complete-ack" && complete != nil)) {
		f.faults++
		f.t.Logf("%s response lost after durable commit job=%d", f.fault, job.GetJobId())

		return f.errLost
	}

	return nil
}

func (f *recoveryFixture) seedLedger(seq uint64) {
	f.t.Helper()
	batch := f.store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(seq).Build()
	log := &ledgerpb.Log{Sequence: seq, Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_CreateLedger{
		CreateLedger: &ledgerpb.CreatedLedgerLog{Name: fmt.Sprintf("ledger-%d", seq), Id: uint32(seq)},
	}}}
	require.NoError(f.t, batch.SetProto(key, log))
	require.NoError(f.t, batch.Commit())
	// Populate the logical projection alongside its history. This fixture does
	// not assert that a full audit passes.
	projection := f.store.OpenWriteSession()
	require.NoError(f.t, state.SaveLedger(projection, fmt.Sprintf("ledger-%d", seq),
		&commonpb.LedgerInfo{Id: uint32(seq)}))
	require.NoError(f.t, projection.Commit())
}

func (f *recoveryFixture) full() (*infrabackup.Result, error) {
	return f.orchestrator.RunFull(context.Background(), f.dst, f.storage)
}

func (f *recoveryFixture) assertActive(jobID uint64) {
	f.t.Helper()
	active := f.jobs.ActiveByDestination(f.dst)
	require.NotNil(f.t, active)
	require.Equal(f.t, jobID, active.GetJobId())
	require.Equal(f.t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_RUNNING, active.GetStatus())
	reader, err := f.store.NewDirectReadHandle()
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, reader.Close()) }()
	reloaded := state.NewBackupJobsState()
	require.NoError(f.t, reloaded.RestoreFromStore(reader))
	require.Equal(f.t, f.jobs.Snapshot(), reloaded.Snapshot(), "RUNNING must be durable, not just in memory")
	require.False(f.t, f.orchestrator.Registry().IsAlive(jobID), "returned attempt has no live executor")
}

func (f *recoveryFixture) assertBusy(jobID uint64) {
	f.t.Helper()
	before := maps.Clone(f.objects)
	puts, deletes := f.puts, f.deletes
	_, err := f.full()
	require.ErrorIs(f.t, err, state.ErrBackupInProgress)
	require.Equal(f.t, before, f.objects, "busy attempt must not modify published bytes")
	require.Equal(f.t, puts, f.puts)
	require.Equal(f.t, deletes, f.deletes)
	f.assertActive(jobID)
	f.t.Logf("same destination busy job=%d; storage untouched", jobID)
}

func (f *recoveryFixture) recoverOrphan(jobID uint64) {
	f.t.Helper()
	f.cleanupFails = 1
	f.cleanup.tick(context.Background())
	f.assertActive(jobID)
	f.cleanup.tick(context.Background())
	require.Equal(f.t, 2, f.cleanupAttempts)
	require.Empty(f.t, f.jobs.Snapshot())
	f.t.Logf("orphan recovered after exactly two cleanup attempts job=%d", jobID)
}

func (f *recoveryFixture) assertFinished() {
	f.t.Helper()
	require.Equal(f.t, 1, f.faults)
	require.Empty(f.t, f.jobs.Snapshot())
	reader, err := f.store.NewDirectReadHandle()
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, reader.Close()) }()
	reloaded := state.NewBackupJobsState()
	require.NoError(f.t, reloaded.RestoreFromStore(reader))
	require.Empty(f.t, reloaded.Snapshot(), "no durable active slot survives terminal commit")
	for _, jobID := range f.starts {
		require.False(f.t, f.orchestrator.Registry().IsAlive(jobID))
		_, exists := f.store.TemporaryCheckpointPath(fmt.Sprintf("backup-%016x", jobID))
		require.False(f.t, exists, "temporary checkpoint must be removed for job %d", jobID)
	}
	prefix := []byte{dal.ZoneClusterTransient, dal.SubTransientBackupJobHistory}
	iter, err := dal.NewBoundedIter(reader, prefix, []byte{dal.ZoneClusterTransient, dal.SubTransientBackupJobHistory + 1})
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, iter.Close()) }()
	var history []*raftcmdpb.BackupJob
	for iter.First(); iter.Valid(); iter.Next() {
		job := &raftcmdpb.BackupJob{}
		require.NoError(f.t, job.UnmarshalVT(iter.Value()))
		history = append(history, job)
	}
	require.NoError(f.t, iter.Error())
	require.Equal(f.t, f.terminals, history, "terminal history must persist every success and cleanup failure")
	require.Equal(f.t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_COMPLETE, history[len(history)-1].GetStatus())
}

func (f *recoveryFixture) assertRestorable(lastSeq uint64) {
	f.t.Helper()
	manifest, err := infrabackup.ReadManifest(context.Background(), f.storage, infrabackup.ManifestKey(f.dst.GetBucketId()))
	require.NoError(f.t, err)
	require.NotEmpty(f.t, manifest.Checkpoint.Files)
	require.Equal(f.t, lastSeq, manifest.Checkpoint.LastLogSequence)
	require.Empty(f.t, manifest.Exports)
	checkpoint := f.t.TempDir()
	for name, file := range manifest.Checkpoint.Files {
		body, ok := f.objects[file.Key]
		require.True(f.t, ok, "checkpoint reference %s must exist", file.Key)
		require.Equal(f.t, file.Size, int64(len(body)))
		require.True(f.t, strings.HasSuffix(file.Key, fmt.Sprintf(".%x", sha256.Sum256(body))), "content-address checksum must match")
		require.NoError(f.t, os.WriteFile(filepath.Join(checkpoint, name), body, 0600))
	}
	restored, err := dal.OpenDirect(checkpoint, logging.Testing())
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, restored.Close()) }()
	reader, err := restored.NewDirectReadHandle()
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, reader.Close()) }()
	for seq := uint64(1); seq <= lastSeq; seq++ {
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(seq).Build()
		want, err := dal.GetValue(f.store, key)
		require.NoError(f.t, err)
		got, err := dal.GetValue(reader, key)
		require.NoError(f.t, err)
		require.Equal(f.t, want, got, "restored history bytes at sequence %d", seq)
		ledger, err := query.GetLedgerByName(context.Background(), reader, fmt.Sprintf("ledger-%d", seq))
		require.NoError(f.t, err)
		require.NotNil(f.t, ledger, "restored business projection for sequence %d", seq)
		require.Equal(f.t, uint32(seq), ledger.GetId())
		live, err := query.GetLedgerByName(context.Background(), f.store, fmt.Sprintf("ledger-%d", seq))
		require.NoError(f.t, err)
		require.Equal(f.t, live, ledger, "restored ledger projection must match live state")
	}
	f.t.Logf("restored checkpoint: exact history bytes and %d ledgers", lastSeq)
}

func TestBackupRecovery_FullUploadFailureLostFail(t *testing.T) {
	t.Parallel()
	f := newRecoveryFixture(t)
	f.seedLedger(1)
	f.failPut, f.fault = true, "lost-fail"
	result, err := f.full()
	require.Nil(t, result)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, f.putFailures)
	_, published := f.objects[infrabackup.ManifestKey(f.dst.GetBucketId())]
	require.False(t, published, "failed upload must not publish a manifest")
	jobID := f.starts[0]
	f.assertActive(jobID)
	f.assertBusy(jobID)
	f.recoverOrphan(jobID)
	result, err = f.full()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Positive(t, result.FilesUploaded)
	require.Equal(t, uint64(1), result.LastLogSequence)
	require.Len(t, f.starts, 3, "failed upload, busy attempt, successful retry")
	require.Len(t, f.terminals, 2)
	require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_FAILED, f.terminals[0].GetStatus())
	f.assertFinished()
	f.assertRestorable(1)
}
