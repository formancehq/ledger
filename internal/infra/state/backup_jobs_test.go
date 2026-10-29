package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func destA() *raftcmdpb.BackupDestination {
	return &raftcmdpb.BackupDestination{
		BucketId: "ledger-a",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{
				Bucket: "ledger-backups",
				Region: "eu-west-1",
			},
		},
	}
}

func destB() *raftcmdpb.BackupDestination {
	return &raftcmdpb.BackupDestination{
		BucketId: "ledger-b",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{
				Bucket: "ledger-backups",
				Region: "eu-west-1",
			},
		},
	}
}

func TestCanonicalDestinationKey_Determinism(t *testing.T) {
	t.Parallel()

	require.Equal(t, CanonicalDestinationKey(destA()), CanonicalDestinationKey(destA()),
		"same destination must produce identical keys")
	require.NotEqual(t, CanonicalDestinationKey(destA()), CanonicalDestinationKey(destB()),
		"different destinations must produce different keys")
	require.Len(t, CanonicalDestinationKey(destA()), 32, "sha256 digest is 32 bytes")
}

// TestCanonicalDestinationKey_S3AWSEndpointVariantsFold guards the
// AWS-endpoint canonicalisation: every spelling of an AWS S3 endpoint
// — empty, global, regional, accelerated, dual-stack, FIPS, with or
// without trailing slash — points at the same global bucket namespace
// (AWS bucket names are globally unique). All forms must hash to the
// same destination_key so a caller switching between "leave blank"
// and "type the full URL" still shares the FSM mutex.
func TestCanonicalDestinationKey_S3AWSEndpointVariantsFold(t *testing.T) {
	t.Parallel()

	mk := func(endpoint string) *raftcmdpb.BackupDestination {
		return &raftcmdpb.BackupDestination{
			BucketId: "b",
			Target: &raftcmdpb.BackupDestination_S3{
				S3: &raftcmdpb.S3BackupTarget{
					Bucket:   "ledger-backups",
					Endpoint: endpoint,
				},
			},
		}
	}

	awsForms := []string{
		"",
		"https://s3.amazonaws.com",
		"https://s3.amazonaws.com/",
		"https://s3.us-east-1.amazonaws.com",
		"https://s3.eu-west-3.amazonaws.com/",
		"https://s3-accelerate.amazonaws.com",
		"https://s3.dualstack.us-east-1.amazonaws.com",
		"https://s3-fips.us-east-1.amazonaws.com",
		"http://s3.amazonaws.com",
	}

	base := CanonicalDestinationKey(mk(awsForms[0]))
	for _, form := range awsForms[1:] {
		require.Equal(t, base, CanonicalDestinationKey(mk(form)),
			"AWS S3 endpoint variant %q must fold to the same key", form)
	}

	// MinIO / non-AWS endpoint must NOT collapse with AWS — the
	// endpoint really does select the namespace there.
	require.NotEqual(t, base, CanonicalDestinationKey(mk("http://minio:9000")),
		"non-AWS endpoint must keep its own destination slot")
}

// TestCanonicalDestinationKey_S3RegionIgnored guards the deliberate
// choice to exclude s3_region from the destination key. Two callers
// that differ only on region target the same physical bucket — AWS
// bucket names are globally unique, and on MinIO/S3-compatible setups
// region is pure SDK metadata. Hashing region would let those callers
// bypass the FSM destination mutex while racing the same manifest.
func TestCanonicalDestinationKey_S3RegionIgnored(t *testing.T) {
	t.Parallel()

	mk := func(region string) *raftcmdpb.BackupDestination {
		return &raftcmdpb.BackupDestination{
			BucketId: "b",
			Target: &raftcmdpb.BackupDestination_S3{
				S3: &raftcmdpb.S3BackupTarget{
					Bucket:   "ledger-backups",
					Region:   region,
					Endpoint: "http://minio:9000",
				},
			},
		}
	}

	require.Equal(t,
		CanonicalDestinationKey(mk("us-east-1")),
		CanonicalDestinationKey(mk("eu-west-1")),
		"region must not affect the destination key")
	require.Equal(t,
		CanonicalDestinationKey(mk("us-east-1")),
		CanonicalDestinationKey(mk("")),
		"empty region must hash same as any explicit region")
}

// TestCanonicalDestinationKey_S3EndpointTrailingSlash guards against
// the same trailing-slash divergence on the S3 path: MinIO and S3
// clients treat `http://minio:9000` and `http://minio:9000/` as the
// same endpoint, so the FSM destination key must too.
func TestCanonicalDestinationKey_S3EndpointTrailingSlash(t *testing.T) {
	t.Parallel()

	mk := func(endpoint string) *raftcmdpb.BackupDestination {
		return &raftcmdpb.BackupDestination{
			BucketId: "b",
			Target: &raftcmdpb.BackupDestination_S3{
				S3: &raftcmdpb.S3BackupTarget{
					Bucket:   "ledger-backups",
					Region:   "us-east-1",
					Endpoint: endpoint,
				},
			},
		}
	}

	require.Equal(t,
		CanonicalDestinationKey(mk("http://minio:9000")),
		CanonicalDestinationKey(mk("http://minio:9000/")),
		"trailing slash must not split the destination slot")
}

// TestStart_ClonesDestination guards against the pooled-pointer
// aliasing bug. start.GetDestination() comes from a vtprotobuf-pooled
// Proposal that is reset/reused after apply; without a clone, mutating
// the source after Start returns would corrupt the FSM's view of the
// active job (and any history record we later write).
func TestStart_ClonesDestination(t *testing.T) {
	t.Parallel()

	s := NewBackupJobsState()
	src := destA()

	job, _, err := s.Start(nil, 100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 42, Destination: src,
	})
	require.NoError(t, err)
	require.Equal(t, src.GetBucketId(), job.GetDestination().GetBucketId())

	// Mutate the source under the FSM. A pointer-aliased Destination
	// would surface "tampered" downstream; a cloned Destination keeps
	// the original BucketId.
	src.BucketId = "tampered"
	src.GetS3().Bucket = "tampered"

	stored := s.ActiveByDestination(destA())
	require.NotNil(t, stored, "the original destination must still be addressable")
	require.NotEqual(t, "tampered", stored.GetDestination().GetBucketId())
	require.NotEqual(t, "tampered", stored.GetDestination().GetS3().GetBucket())
}

// TestCanonicalDestinationKey_AzureEndpointForms guards against the
// drift between three spellings of the same Azure default endpoint —
// empty, default URL with trailing slash, default URL without trailing
// slash. All three resolve to the same container at runtime and must
// hash to the same destination_key, otherwise the FSM mutex would not
// catch concurrent writes to the same physical storage.
func TestCanonicalDestinationKey_AzureEndpointForms(t *testing.T) {
	t.Parallel()

	mk := func(endpoint string) *raftcmdpb.BackupDestination {
		return &raftcmdpb.BackupDestination{
			BucketId: "b",
			Target: &raftcmdpb.BackupDestination_Azure{
				Azure: &raftcmdpb.AzureBackupTarget{
					AccountName: "acct",
					Container:   "backups",
					Endpoint:    endpoint,
				},
			},
		}
	}

	empty := CanonicalDestinationKey(mk(""))
	withSlash := CanonicalDestinationKey(mk("https://acct.blob.core.windows.net/"))
	noSlash := CanonicalDestinationKey(mk("https://acct.blob.core.windows.net"))

	require.Equal(t, empty, withSlash, "empty and default-with-slash must hash identically")
	require.Equal(t, empty, noSlash, "empty and default-without-slash must hash identically")
}

func TestCanonicalDestinationKey_FieldSensitivity(t *testing.T) {
	t.Parallel()

	// Each NAMESPACE-determining field flip must yield a different key.
	// Region is excluded — see TestCanonicalDestinationKey_S3RegionIgnored.
	base := destA()
	s3Base := base.GetS3()
	mutated := []*raftcmdpb.BackupDestination{
		// Switching the oneof case must flip the key — Azure with empty
		// fields must NOT collide with an empty-fields S3 destination.
		{BucketId: base.GetBucketId(), Target: &raftcmdpb.BackupDestination_Azure{Azure: &raftcmdpb.AzureBackupTarget{}}},
		{BucketId: "other", Target: &raftcmdpb.BackupDestination_S3{S3: s3Base}},
		{BucketId: base.GetBucketId(), Target: &raftcmdpb.BackupDestination_S3{S3: &raftcmdpb.S3BackupTarget{Bucket: "other", Region: s3Base.GetRegion()}}},
		{BucketId: base.GetBucketId(), Target: &raftcmdpb.BackupDestination_S3{S3: &raftcmdpb.S3BackupTarget{Bucket: s3Base.GetBucket(), Region: s3Base.GetRegion(), Endpoint: "https://minio.local"}}},
	}

	baseKey := CanonicalDestinationKey(base)
	for _, m := range mutated {
		require.NotEqual(t, baseKey, CanonicalDestinationKey(m),
			"mutated field must change the destination key: %+v", m)
	}
}

// TestCanonicalDestinationKey_BasePathIgnored guards the deliberate
// choice to exclude base_path from the destination key: the inner
// backup runners ignore it (it is reserved for future use), so two
// callers with the same bucket but different base_paths must contend
// for the same FSM slot. Otherwise mutual exclusion would be bypassed
// by passing distinct base_path values for the same actual manifest.
func TestCanonicalDestinationKey_BasePathIgnored(t *testing.T) {
	t.Parallel()

	a := destA()
	a.BasePath = "/one"

	b := destA()
	b.BasePath = "/two"

	require.Equal(t, CanonicalDestinationKey(a), CanonicalDestinationKey(b),
		"base_path must not affect the destination key")
}

func TestBackupJobsState_StartRejectsBusyDestination(t *testing.T) {
	t.Parallel()

	s := NewBackupJobsState()

	first := &raftcmdpb.BackupOrderStart{
		JobId:          1,
		Destination:    destA(),
		ExecutorNodeId: 7,
	}
	_, ok, err := s.Start(nil, 1000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, first)
	require.NoError(t, err)
	require.True(t, ok)

	second := &raftcmdpb.BackupOrderStart{
		JobId:          2,
		Destination:    destA(),
		ExecutorNodeId: 7,
	}
	_, ok, err = s.Start(nil, 1100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, second)
	require.ErrorIs(t, err, ErrBackupInProgress)
	require.False(t, ok)
}

// TestBackupJobsState_StartRejectsDuplicateJobID exercises the
// invariant that job_id is unique across all active destinations:
// without it, findActiveByJobID has two candidates, the choice depends
// on Go map iteration order, and nodes diverge for the same applied
// entry. ErrBackupJobIDCollision is the defensive reply when the
// orchestrator's RNG (or any future non-random source) hands out a
// duplicate.
func TestBackupJobsState_StartRejectsDuplicateJobID(t *testing.T) {
	t.Parallel()

	s := NewBackupJobsState()

	_, ok, err := s.Start(nil, 1000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 42, Destination: destA(),
	})
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = s.Start(nil, 1100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 42, Destination: destB(),
	})
	require.ErrorIs(t, err, ErrBackupJobIDCollision)
	require.False(t, ok)
}

func TestBackupJobsState_DifferentDestinationsRunInParallel(t *testing.T) {
	t.Parallel()

	s := NewBackupJobsState()

	_, ok, err := s.Start(nil, 1000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 1, Destination: destA(), ExecutorNodeId: 7,
	})
	require.NoError(t, err)
	require.True(t, ok)

	_, ok, err = s.Start(nil, 1010, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 2, Destination: destB(), ExecutorNodeId: 7,
	})
	require.NoError(t, err, "two destinations must run in parallel")
	require.True(t, ok)

	snap := s.Snapshot()
	require.Len(t, snap, 2)
}

func TestBackupJobsState_FullAndIncrementalShareDestinationSlot(t *testing.T) {
	t.Parallel()

	// A full and an incremental backup pointing at the same destination
	// still contend on the same slot — same on-S3 manifest, same lock.
	s := NewBackupJobsState()

	_, _, err := s.Start(nil, 1000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 1, Destination: destA(),
	})
	require.NoError(t, err)

	_, ok, err := s.Start(nil, 1100, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, &raftcmdpb.BackupOrderStart{
		JobId: 2, Destination: destA(),
	})
	require.ErrorIs(t, err, ErrBackupInProgress)
	require.False(t, ok)
}

func TestBackupJobsState_CompleteCarriesFinalCounters(t *testing.T) {
	t.Parallel()

	// Complete is the only path that records the upload totals on the
	// history record — Progress orders were removed when liveness moved
	// to the leader-local registry.
	s := NewBackupJobsState()
	_, _, err := s.Start(nil, 1000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 42, Destination: destA(),
	})
	require.NoError(t, err)

	completed, err := s.Complete(nil, 2000, &raftcmdpb.BackupOrderComplete{
		JobId:             42,
		LastLogSequence:   12345,
		LastAuditSequence: 6789,
		FilesUploaded:     17,
		SegmentsUploaded:  3,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(17), completed.GetFilesUploaded())
	require.Equal(t, uint64(3), completed.GetSegmentsUploaded())
}

func TestBackupJobsState_CompleteFreesDestinationSlot(t *testing.T) {
	t.Parallel()

	s := NewBackupJobsState()
	_, _, err := s.Start(nil, 1000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 42, Destination: destA(),
	})
	require.NoError(t, err)

	completed, err := s.Complete(nil, 2000, &raftcmdpb.BackupOrderComplete{
		JobId:             42,
		LastLogSequence:   12345,
		LastAuditSequence: 6789,
	})
	require.NoError(t, err)
	require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_COMPLETE, completed.GetStatus())
	require.Equal(t, uint64(2000), completed.GetCompletedAtAppliedIndex())
	require.Empty(t, s.Snapshot(), "active map must be empty after Complete")

	// A new Start for the same destination is allowed.
	_, ok, err := s.Start(nil, 2100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 43, Destination: destA(),
	})
	require.NoError(t, err)
	require.True(t, ok, "destination slot must be free after Complete")
}

func TestBackupJobsState_FailFreesDestinationSlot(t *testing.T) {
	t.Parallel()

	s := NewBackupJobsState()
	_, _, err := s.Start(nil, 1000, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 42, Destination: destA(),
	})
	require.NoError(t, err)

	failed, err := s.Fail(nil, 2000, &raftcmdpb.BackupOrderFail{
		JobId:   42,
		Message: "s3 upload timed out",
	})
	require.NoError(t, err)
	require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_FAILED, failed.GetStatus())
	require.Equal(t, "s3 upload timed out", failed.GetFailureMessage())
	require.Empty(t, s.Snapshot())
}

func TestBackupJobsState_RestoreFromStore(t *testing.T) {
	t.Parallel()

	// Spin up a Pebble store, write two active jobs via the apply path,
	// reset the in-memory state, restore — the map must come back with
	// the same entries.
	store := newTempDalStore(t)

	s := NewBackupJobsState()

	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	_, _, err := s.Start(batch, 100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 1, Destination: destA(), ExecutorNodeId: 7,
	})
	require.NoError(t, err)

	_, _, err = s.Start(batch, 110, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, &raftcmdpb.BackupOrderStart{
		JobId: 2, Destination: destB(), ExecutorNodeId: 7,
	})
	require.NoError(t, err)

	require.NoError(t, batch.Commit())

	s.Reset()
	require.Empty(t, s.Snapshot())

	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	err = s.RestoreFromStore(reader)
	_ = reader.Close()
	require.NoError(t, err)

	restored := s.Snapshot()
	require.Len(t, restored, 2)

	// Look up by destination to assert content survived round-trip.
	rA := s.ActiveByDestination(destA())
	require.NotNil(t, rA)
	require.Equal(t, uint64(1), rA.GetJobId())
	require.Equal(t, raftcmdpb.BackupKind_BACKUP_KIND_FULL, rA.GetKind())

	rB := s.ActiveByDestination(destB())
	require.NotNil(t, rB)
	require.Equal(t, uint64(2), rB.GetJobId())
	require.Equal(t, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, rB.GetKind())
}

func TestBackupJobsState_RestoreSkipsCompletedJobs(t *testing.T) {
	t.Parallel()

	// Active jobs disappear when Complete commits — RestoreFromStore
	// must not bring them back from the history zone.
	store := newTempDalStore(t)

	s := NewBackupJobsState()
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	_, _, err := s.Start(batch, 100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 1, Destination: destA(),
	})
	require.NoError(t, err)

	_, err = s.Complete(batch, 200, &raftcmdpb.BackupOrderComplete{JobId: 1})
	require.NoError(t, err)

	require.NoError(t, batch.Commit())

	s.Reset()
	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	err = s.RestoreFromStore(reader)
	_ = reader.Close()
	require.NoError(t, err)
	require.Empty(t, s.Snapshot(), "completed jobs must NOT come back as active on restore")
}

// TestBackupJobsState_HistoryKeyTieBreaker guards against two terminal
// jobs sharing a CompletedAtAppliedIndex (a single proposal can carry
// both a BackupOrder and an IncrementalBackupOrder, so the FSM may
// terminate two jobs under the same applied index). The destination
// key suffix on the history key keeps both rows distinct in Pebble.
func TestBackupJobsState_HistoryKeyTieBreaker(t *testing.T) {
	t.Parallel()

	store := newTempDalStore(t)

	s := NewBackupJobsState()
	batch := store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	_, _, err := s.Start(batch, 100, raftcmdpb.BackupKind_BACKUP_KIND_FULL, &raftcmdpb.BackupOrderStart{
		JobId: 1, Destination: destA(),
	})
	require.NoError(t, err)

	_, _, err = s.Start(batch, 110, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, &raftcmdpb.BackupOrderStart{
		JobId: 2, Destination: destB(),
	})
	require.NoError(t, err)

	// Both jobs Complete under the same raftIndex — the proto-level
	// shape that motivates the tie-breaker.
	_, err = s.Complete(batch, 300, &raftcmdpb.BackupOrderComplete{JobId: 1})
	require.NoError(t, err)
	_, err = s.Complete(batch, 300, &raftcmdpb.BackupOrderComplete{JobId: 2})
	require.NoError(t, err)

	require.NoError(t, batch.Commit())

	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	keyA := backupJobHistoryKey(300, CanonicalDestinationKey(destA()))
	keyB := backupJobHistoryKey(300, CanonicalDestinationKey(destB()))
	require.NotEqual(t, keyA, keyB, "history keys must differ when destinations differ")

	for _, key := range [][]byte{keyA, keyB} {
		_, closer, err := reader.Get(key)
		require.NoError(t, err, "history row must survive at %x", key)
		_ = closer.Close()
	}
}

// newTempDalStore returns a Pebble store rooted at t.TempDir, closed on
// test cleanup. Used by the restore tests above.
func newTempDalStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}
