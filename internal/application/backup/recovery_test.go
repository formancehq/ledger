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

	infrabackup "github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -destination=recovery_storage_generated_test.go -package=backup github.com/formancehq/ledger/v3/internal/infra/backup Storage

// recoveryFixture injects one transport failure at the Proposer boundary. It
// executes and commits the real job-state transitions, not Raft elections. The
// orchestration, checkpoint/export work and cleanup scan remain real.
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
			// Drain streaming exports even when injecting failure, so the pipe
			// writer can terminate. Calls are serialized by the backup runner.
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
	if order := extractFullOrder(proposal); order != nil {
		start, complete, fail = order.GetStart(), order.GetComplete(), order.GetFail()
	} else {
		order := extractIncrementalOrder(proposal)
		require.NotNil(f.t, order)
		kind = raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL
		start, complete, fail = order.GetStart(), order.GetComplete(), order.GetFail()
	}
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
	log := &commonpb.Log{Sequence: seq, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
		CreateLedger: &commonpb.CreatedLedgerLog{Name: fmt.Sprintf("ledger-%d", seq), Id: uint32(seq)},
	}}}
	require.NoError(f.t, batch.SetProto(key, log))
	require.NoError(f.t, batch.Commit())
	// Populate the logical projection as well as its raw history. This is a
	// small valid replay fixture, not an assertion that a full audit passes.
	require.NoError(f.t, infrabackup.RebuildDelta(context.Background(), logging.Testing(), f.store, seq-1, 0))
}

func (f *recoveryFixture) full() (*infrabackup.Result, error) {
	return f.orchestrator.RunFull(context.Background(), f.dst, f.storage)
}

func (f *recoveryFixture) incremental() (*infrabackup.IncrementalBackupResult, error) {
	return f.orchestrator.RunIncremental(context.Background(), f.dst, f.storage)
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

func (f *recoveryFixture) assertBusy(incremental bool, jobID uint64) {
	f.t.Helper()
	before := maps.Clone(f.objects)
	puts, deletes := f.puts, f.deletes
	var err error
	if incremental {
		_, err = f.incremental()
	} else {
		_, err = f.full()
	}
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

func (f *recoveryFixture) assertRestorable(lastSeq uint64, incremental bool) {
	f.t.Helper()
	manifest, err := infrabackup.ReadManifest(context.Background(), f.storage, infrabackup.ManifestKey(f.dst.GetBucketId()))
	require.NoError(f.t, err)
	require.NotEmpty(f.t, manifest.Checkpoint.Files)
	require.Equal(f.t, lastSeq, manifest.LastExportLogSequence())
	if incremental {
		require.NotEmpty(f.t, manifest.Exports)
		require.Greater(f.t, lastSeq, manifest.Checkpoint.LastLogSequence)
	}
	checkpoint := f.t.TempDir()
	for name, file := range manifest.Checkpoint.Files {
		body, ok := f.objects[file.Key]
		require.True(f.t, ok, "checkpoint reference %s must exist", file.Key)
		require.Equal(f.t, file.Size, int64(len(body)))
		require.True(f.t, strings.HasSuffix(file.Key, fmt.Sprintf(".%x", sha256.Sum256(body))), "content-address checksum must match")
		require.NoError(f.t, os.WriteFile(filepath.Join(checkpoint, name), body, 0600))
	}
	for _, segment := range manifest.Exports {
		body, ok := f.objects[segment.Key]
		require.True(f.t, ok, "export reference %s must exist", segment.Key)
		require.Equal(f.t, segment.Size, int64(len(body)))
	}
	restored, err := dal.OpenDirect(checkpoint, logging.Testing())
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, restored.Close()) }()
	require.NoError(f.t, infrabackup.ApplyExportsAndRebuild(context.Background(), logging.Testing(), f.storage, restored, manifest))
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
	f.t.Logf("restored checkpoint + %d exports: exact history bytes and %d ledgers", len(manifest.Exports), lastSeq)
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
	f.assertBusy(false, jobID)
	f.assertBusy(true, jobID) // full and incremental share the destination slot
	f.recoverOrphan(jobID)
	result, err = f.full()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Positive(t, result.FilesUploaded)
	require.Equal(t, uint64(1), result.LastLogSequence)
	require.Len(t, f.starts, 4, "failed upload, two busy attempts, successful retry")
	require.Len(t, f.terminals, 2)
	require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_FAILED, f.terminals[0].GetStatus())
	f.assertFinished()
	f.assertRestorable(1, false)
}

func TestBackupRecovery_IncrementalAmbiguousProposal(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"lost-complete", "start-ack", "complete-ack"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			f := newRecoveryFixture(t)
			f.seedLedger(1)
			_, err := f.full()
			require.NoError(t, err)
			f.seedLedger(2)
			first, err := f.incremental()
			require.NoError(t, err)
			require.Equal(t, uint64(1), first.LogEntriesExported)
			f.seedLedger(3)
			f.fault = fault
			putsBefore := f.puts
			result, err := f.incremental()
			require.Nil(t, result)
			require.ErrorIs(t, err, f.errLost)
			published, manifestErr := infrabackup.ReadManifest(context.Background(), f.storage, infrabackup.ManifestKey(f.dst.GetBucketId()))
			require.NoError(t, manifestErr)
			if fault == "start-ack" {
				require.Equal(t, putsBefore, f.puts, "lost Start acknowledgment must exit before upload")
				require.Equal(t, uint64(2), published.LastExportLogSequence())
			} else {
				require.Greater(t, f.puts, putsBefore, "Complete failure happens after export publication")
				require.Equal(t, uint64(3), published.LastExportLogSequence())
			}
			jobID := f.starts[2]
			if fault == "complete-ack" {
				require.Empty(t, f.jobs.Snapshot(), "lost response after Complete commit must not retain the slot")
				f.cleanup.tick(context.Background())
				require.Zero(t, f.cleanupAttempts)
			} else {
				f.assertActive(jobID)
				f.assertBusy(true, jobID)
				f.recoverOrphan(jobID)
			}
			// A new committed log makes the retry do real export work even if
			// the failed attempt already published sequence 3's manifest.
			f.seedLedger(4)
			result, err = f.incremental()
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Positive(t, result.SegmentsUploaded)
			require.Equal(t, uint64(4), result.LastLogSequence)
			exported := uint64(1)
			if fault == "start-ack" {
				exported = 2
			}
			require.Equal(t, exported, result.LogEntriesExported)
			attempts := 5
			if fault == "complete-ack" {
				attempts = 4
			}
			require.Len(t, f.starts, attempts)
			require.Len(t, f.terminals, 4)
			if fault != "complete-ack" {
				require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_FAILED, f.terminals[2].GetStatus())
			}
			f.assertFinished()
			f.assertRestorable(4, true)
		})
	}
}
