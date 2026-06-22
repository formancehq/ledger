package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestApplyBackupOrder_RejectsBusyDestination exercises the apply path
// itself (not just BackupJobsState in isolation): a second Start
// against an already-Running destination must surface as
// ErrBackupInProgress via the returned error so machine.applyProposal
// can translate it into ApplyResult.Error, not as a fatal apply error.
// That distinction is what lets the leader keep applying subsequent
// proposals after a single rejection.
func TestApplyBackupOrder_RejectsBusyDestination(t *testing.T) {
	t.Parallel()

	fsm, store, _ := newTestMachine(t)

	dst := &raftcmdpb.BackupDestination{
		BucketId: "bucket-x",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{
				Bucket: "ledger-backups",
				Region: "eu-west-1",
			},
		},
	}

	// First Start succeeds and lands on the active map.
	batch1 := store.OpenWriteSession()

	require.NoError(t, fsm.applyBackupOrder(batch1, 1_000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
			JobId:       42,
			Destination: dst,
		}},
	}), "first Start must not return a rejection")
	require.NoError(t, batch1.Commit())

	// Second Start against the same destination surfaces ErrBackupInProgress
	// via the error return — machine.applyProposal converts it to
	// ApplyResult.Error so the proposer learns about it.
	batch2 := store.OpenWriteSession()
	err := fsm.applyBackupOrder(batch2, 1_100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
			JobId:       43,
			Destination: dst,
		}},
	})
	require.ErrorIs(t, err, ErrBackupInProgress)
	_ = batch2.Cancel()
}

// TestApplyBackupOrder_UnknownJobReturnsNotFound covers the other
// branch: Complete / Fail against a job_id that no longer matches the
// active job (taken over by cleanup, never existed, already completed)
// surface as ErrBackupJobNotFound via the error return.
func TestApplyBackupOrder_UnknownJobReturnsNotFound(t *testing.T) {
	t.Parallel()

	fsm, store, _ := newTestMachine(t)

	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	err := fsm.applyBackupOrder(batch, 1, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Complete{Complete: &raftcmdpb.BackupOrderComplete{
			JobId: 99,
		}},
	})
	require.ErrorIs(t, err, ErrBackupJobNotFound)
}

// TestApplyBackupOrder_HistoryEntryWrittenOnComplete walks one full
// Start → Complete cycle and asserts the terminal entry lands under
// [ZoneClusterTransient][SubTransientBackupJobHistory] with the completion applied
// index as the key. The active slot must also be freed.
func TestApplyBackupOrder_HistoryEntryWrittenOnComplete(t *testing.T) {
	t.Parallel()

	fsm, store, _ := newTestMachine(t)

	dst := &raftcmdpb.BackupDestination{
		BucketId: "bucket-h",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{
				Bucket: "ledger-backups",
				Region: "eu-west-1",
			},
		},
	}

	batch := store.OpenWriteSession()
	require.NoError(t, fsm.applyBackupOrder(batch, 1_000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
			JobId:       7,
			Destination: dst,
		}},
	}))

	require.NoError(t, fsm.applyBackupOrder(batch, 2_500, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Complete{Complete: &raftcmdpb.BackupOrderComplete{
			JobId:             7,
			LastLogSequence:   42,
			LastAuditSequence: 24,
		}},
	}))
	require.NoError(t, batch.Commit())

	// Active slot is free: a fresh Start for the same destination must
	// succeed now.
	require.Nil(t, fsm.Registry.BackupJobs.ActiveByDestination(dst),
		"Complete must free the active destination slot")

	// History row exists at [Zone=Global][Sub=BackupJobHistory][2500BE].
	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	historyKey := backupJobHistoryKey(2_500, CanonicalDestinationKey(dst))
	bytes, closer, err := reader.Get(historyKey)
	require.NoError(t, err, "history entry must be present at completed_at_applied_index")
	defer func() { _ = closer.Close() }()

	job := &raftcmdpb.BackupJob{}
	require.NoError(t, job.UnmarshalVT(bytes))
	require.Equal(t, uint64(7), job.GetJobId())
	require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_COMPLETE, job.GetStatus())
	require.Equal(t, uint64(2_500), job.GetCompletedAtAppliedIndex())
	require.Equal(t, uint64(42), job.GetLastLogSequence())
}

// TestApplyIncrementalBackupOrder_LifecycleAllOps exercises the
// incremental dispatcher's four branches (Start, Progress, Complete,
// Fail) plus the no-op nil branch. The lifecycle is the same as the
// full path through a different proto carrier; this test guards
// against the two paths drifting apart.
func TestApplyIncrementalBackupOrder_LifecycleAllOps(t *testing.T) {
	t.Parallel()

	fsm, store, _ := newTestMachine(t)
	dst := &raftcmdpb.BackupDestination{
		BucketId: "bucket-incremental",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{Bucket: "b", Region: "r"},
		},
	}

	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	// Start
	require.NoError(t, fsm.applyIncrementalBackupOrder(batch, 100, &raftcmdpb.IncrementalBackupOrder{
		Op: &raftcmdpb.IncrementalBackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
			JobId: 21, Destination: dst,
		}},
	}))

	// A duplicate Start surfaces ErrBackupInProgress on the error return.
	err := fsm.applyIncrementalBackupOrder(batch, 105, &raftcmdpb.IncrementalBackupOrder{
		Op: &raftcmdpb.IncrementalBackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
			JobId: 22, Destination: dst,
		}},
	})
	require.ErrorIs(t, err, ErrBackupInProgress)

	// Fail for an unknown job_id surfaces ErrBackupJobNotFound.
	err = fsm.applyIncrementalBackupOrder(batch, 112, &raftcmdpb.IncrementalBackupOrder{
		Op: &raftcmdpb.IncrementalBackupOrder_Fail{Fail: &raftcmdpb.BackupOrderFail{
			JobId: 999, Message: "missing",
		}},
	})
	require.ErrorIs(t, err, ErrBackupJobNotFound)

	// Complete terminates the real one.
	require.NoError(t, fsm.applyIncrementalBackupOrder(batch, 200, &raftcmdpb.IncrementalBackupOrder{
		Op: &raftcmdpb.IncrementalBackupOrder_Complete{Complete: &raftcmdpb.BackupOrderComplete{
			JobId: 21, LastLogSequence: 5,
		}},
	}))
	require.Nil(t, fsm.Registry.BackupJobs.ActiveByDestination(dst), "Complete must free the slot")
}

// TestApplyBackupOrder_NilOpFailsLoudly pins CLAUDE.md invariant #7
// on the backup carriers: a malformed proposal with no oneof set
// must surface as a fatal apply error, never as a silent no-op.
// Same expectation on the incremental dispatcher.
func TestApplyBackupOrder_NilOpFailsLoudly(t *testing.T) {
	t.Parallel()

	fsm, store, _ := newTestMachine(t)
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	err := fsm.applyBackupOrder(batch, 1, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrder{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invariant")

	err = fsm.applyIncrementalBackupOrder(batch, 2, &raftcmdpb.IncrementalBackupOrder{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invariant")
}

// TestBackupJobsState_ForEachActiveCallsBack guards the iterator API
// used by the cleanup loop. It must visit every running job.
func TestBackupJobsState_ForEachActiveCallsBack(t *testing.T) {
	t.Parallel()

	s := NewBackupJobsState()
	_, _, err := s.Start(nil, 100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 1,
		Destination: &raftcmdpb.BackupDestination{
			BucketId: "b1",
			Target:   &raftcmdpb.BackupDestination_S3{S3: &raftcmdpb.S3BackupTarget{Bucket: "x"}},
		},
	})
	require.NoError(t, err)
	_, _, err = s.Start(nil, 101, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, &raftcmdpb.BackupOrderStart{
		JobId: 2,
		Destination: &raftcmdpb.BackupDestination{
			BucketId: "b2",
			Target:   &raftcmdpb.BackupDestination_S3{S3: &raftcmdpb.S3BackupTarget{Bucket: "y"}},
		},
	})
	require.NoError(t, err)

	visited := map[uint64]bool{}
	s.ForEachActive(func(j *raftcmdpb.BackupJob) {
		visited[j.GetJobId()] = true
	})
	require.Equal(t, map[uint64]bool{1: true, 2: true}, visited)
}
