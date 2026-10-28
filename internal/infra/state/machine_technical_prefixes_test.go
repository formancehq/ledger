package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func technicalCoverageRejection(id uint64, prefix ...*raftcmdpb.TechnicalUpdate) *raftcmdpb.Proposal {
	proposal := makeProposal(id)
	proposal.AtomicTechnicalUpdates = true
	key, _ := attributes.MakeKey((domain.LedgerKey{Name: "atomic"}).Bytes())
	proposal.ExecutionPlan.Attributes = []*raftcmdpb.AttributeCoverage{declareTestPlan(key, dal.SubAttrLedger)}
	proposal.TechnicalUpdates = append(proposal.TechnicalUpdates, prefix...)
	proposal.TechnicalUpdates = append(proposal.TechnicalUpdates, &raftcmdpb.TechnicalUpdate{
		Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "atomic"}},
	})

	return proposal
}

func technicalOrderCoverageRejection(id uint64, prefix ...*raftcmdpb.TechnicalUpdate) *raftcmdpb.Proposal {
	proposal := technicalCoverageRejection(id, prefix...)
	proposal.TechnicalUpdates = prefix
	proposal.Orders = []*raftcmdpb.Order{createLedgerOrder("atomic")}

	return proposal
}

func requireTechnicalCoverageRejection(t *testing.T, err error) {
	t.Helper()
	var miss *ErrCoverageMiss
	require.ErrorAs(t, err, &miss)
	require.Equal(t, "ledgers", miss.Attribute)
}

func TestTechnicalAtomicity_AuditKeyPrefixRejected(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	seedBatch := store.OpenWriteSession()
	require.NoError(t, seedBatch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}))
	require.NoError(t, seedBatch.Commit())
	machine.State.AuditKey = ""
	machine.State.HashGenerator = nil

	previous := makeProposal(1)
	previous.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{sinkCursorUpdate("preserved", 7)}
	rejected := technicalCoverageRejection(2, &raftcmdpb.TechnicalUpdate{
		Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: []byte("fedcba9876543210fedcba9876543210")},
	})
	applied, err := machine.ApplyEntries(t.Context(), store,
		makeEntry(t, 1, previous), unsealedTechnicalEntry(t, 2, rejected))
	require.NoError(t, err)
	require.NoError(t, applied.Results[0].Error)
	requireTechnicalCoverageRejection(t, applied.Results[1].Error)
	require.False(t, applied.Results[1].AuditEntryWritten)
	require.Empty(t, machine.State.AuditKey)
	require.Nil(t, machine.State.HashGenerator)
	key, err := query.ReadAuditKey(store)
	require.NoError(t, err)
	require.Nil(t, key)
	requireTechnicalCursor(t, store, "preserved", 7)
	require.Equal(t, uint64(2), machine.LastAppliedIndex())
	recovered := recoverMachineOnStore(t, store)
	require.Empty(t, recovered.State.AuditKey)
	require.Equal(t, uint64(1), recovered.State.NextAuditSequenceID)
	require.Equal(t, uint64(2), recovered.LastAppliedIndex())
}

func TestTechnicalAtomicity_ClusterConfigPrefixRejected(t *testing.T) {
	t.Parallel()
	for _, orderFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "technical coverage", true: "order coverage"}[orderFailure], func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			cfg := &commonpb.ClusterConfig{RotationThreshold: 1000,
				BloomVolumes: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: .01}}
			seed := makeProposal(1)
			seed.AtomicTechnicalUpdates = true
			seed.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: cfg}}}
			applied, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, seed))
			require.NoError(t, err)
			require.NoError(t, applied.Results[0].Error)
			machine.BloomFilters = bloom.NewFilterSet(cfg, noop.NewMeterProvider().Meter("test"))
			machine.BloomFilters.SetReady(true)
			cacheEpoch, bloomEpoch := machine.Registry.Cache.Epoch(), machine.BloomFilters.Epoch()
			beforeConfig := machine.State.LastClusterConfig.CloneVT()
			reader, err := store.NewReadHandle()
			require.NoError(t, err)
			beforeStored, err := query.ReadClusterState(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())

			changed := cfg.CloneVT()
			changed.RotationThreshold = 10
			changed.BloomVolumes.ExpectedKeys = 2000
			previous := makeProposal(2)
			previous.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{sinkCursorUpdate("preserved", 7)}
			rejected := technicalCoverageRejection(3, &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: changed}})
			if orderFailure {
				rejected = technicalOrderCoverageRejection(3, &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: changed}})
			}
			applied, err = machine.ApplyEntries(t.Context(), store,
				makeEntry(t, 2, previous), unsealedTechnicalEntry(t, 3, rejected))
			require.NoError(t, err)
			require.NoError(t, applied.Results[0].Error)
			requireTechnicalCoverageRejection(t, applied.Results[1].Error)
			require.Equal(t, uint64(1000), machine.Registry.Cache.GenerationThreshold())
			require.Equal(t, cacheEpoch, machine.Registry.Cache.Epoch())
			require.Equal(t, beforeConfig, machine.State.LastClusterConfig)
			require.Equal(t, bloomEpoch, machine.BloomFilters.Epoch())
			require.True(t, machine.BloomFilters.IsReady())
			select {
			case reason := <-machine.BloomRebuildCh():
				t.Fatalf("rejected config requested Bloom rebuild: %s", reason)
			default:
			}
			reader, err = store.NewReadHandle()
			require.NoError(t, err)
			stored, err := query.ReadClusterState(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, beforeStored, stored)
			requireTechnicalCursor(t, store, "preserved", 7)
			require.Equal(t, uint64(3), machine.LastAppliedIndex())
			recovered := recoverMachineOnStore(t, store)
			require.Equal(t, beforeConfig, recovered.State.LastClusterConfig)
			require.Equal(t, cacheEpoch, recovered.Registry.Cache.Epoch())
			require.Equal(t, uint64(1000), recovered.Registry.Cache.GenerationThreshold())
			require.Equal(t, uint64(3), recovered.LastAppliedIndex())
		})
	}
}

func TestTechnicalAtomicity_IdempotencyEvictionPrefixRejected(t *testing.T) {
	t.Parallel()
	for _, orderFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "technical coverage", true: "order coverage"}[orderFailure], func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			const key = "preserved-outcome"
			value := &commonpb.IdempotencyKeyValue{FirstLogSequence: 7, LogCount: 1, Hash: []byte("intent"), CreatedAt: 1, ExpiresAt: 100}
			batch := store.OpenWriteSession()
			require.NoError(t, SaveIdempotencyKey(batch, key, value))
			require.NoError(t, batch.Commit())
			machine.Registry.Idempotency.Put(key, value)
			reader, err := store.NewReadHandle()
			require.NoError(t, err)
			hashes, last, err := machine.Registry.Idempotency.ScanExpiredKeyHashes(reader, 100, 100)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Len(t, hashes, 1)

			previous := makeProposal(1)
			previous.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{sinkCursorUpdate("preserved", 7)}
			rejected := technicalCoverageRejection(2, &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_IdempotencyEviction{
				IdempotencyEviction: &raftcmdpb.IdempotencyEviction{CutoffMicros: 100, PebbleKeyHashes: hashes, LastScannedTimeIndexKey: last},
			}})
			if orderFailure {
				rejected.Orders = []*raftcmdpb.Order{createLedgerOrder("atomic")}
				rejected.TechnicalUpdates = rejected.GetTechnicalUpdates()[:1]
			}
			applied, err := machine.ApplyEntries(t.Context(), store,
				makeEntry(t, 1, previous), unsealedTechnicalEntry(t, 2, rejected))
			require.NoError(t, err)
			require.NoError(t, applied.Results[0].Error)
			requireTechnicalCoverageRejection(t, applied.Results[1].Error)
			got, found := machine.Registry.Idempotency.Get(key)
			require.True(t, found)
			require.Equal(t, value, got)
			require.Zero(t, machine.State.LastIdempotencyEvictionCutoff)
			reader, err = store.NewReadHandle()
			require.NoError(t, err)
			stored, err := LoadIdempotencyKey(reader, key)
			require.NoError(t, err)
			require.Equal(t, value, stored)
			cutoff, err := query.ReadLastIdempotencyEvictionCutoff(reader)
			require.NoError(t, err)
			require.Zero(t, cutoff)
			hashesAfter, lastAfter, err := machine.Registry.Idempotency.ScanExpiredKeyHashes(reader, 100, 100)
			require.NoError(t, err)
			require.Equal(t, hashes, hashesAfter, "rejected eviction must retain its time-index row")
			require.Equal(t, last, lastAfter)
			require.NoError(t, reader.Close())
			requireTechnicalCursor(t, store, "preserved", 7)
			require.Equal(t, uint64(2), machine.LastAppliedIndex())
			recovered := recoverMachineOnStore(t, store)
			got, found = recovered.Registry.Idempotency.Get(key)
			require.True(t, found)
			require.Equal(t, value, got)
			require.Zero(t, recovered.State.LastIdempotencyEvictionCutoff)
			require.Equal(t, uint64(2), recovered.LastAppliedIndex())
		})
	}
}

func technicalBackupUpdate(incremental bool, operation string, jobID uint64, destination *raftcmdpb.BackupDestination) *raftcmdpb.TechnicalUpdate {
	start := &raftcmdpb.BackupOrderStart{JobId: jobID, Destination: destination, ExecutorNodeId: 7}
	complete := &raftcmdpb.BackupOrderComplete{JobId: jobID, LastAppliedIndex: 42, LastLogSequence: 43, LastAuditSequence: 44, FilesUploaded: 45, SegmentsUploaded: 46}
	fail := &raftcmdpb.BackupOrderFail{JobId: jobID, Message: "upload failed"}
	if incremental {
		order := &raftcmdpb.IncrementalBackupOrder{}
		switch operation {
		case "start":
			order.Op = &raftcmdpb.IncrementalBackupOrder_Start{Start: start}
		case "complete":
			order.Op = &raftcmdpb.IncrementalBackupOrder_Complete{Complete: complete}
		case "fail":
			order.Op = &raftcmdpb.IncrementalBackupOrder_Fail{Fail: fail}
		}

		return &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_IncrementalBackupOrder{IncrementalBackupOrder: order}}
	}
	order := &raftcmdpb.BackupOrder{}
	switch operation {
	case "start":
		order.Op = &raftcmdpb.BackupOrder_Start{Start: start}
	case "complete":
		order.Op = &raftcmdpb.BackupOrder_Complete{Complete: complete}
	case "fail":
		order.Op = &raftcmdpb.BackupOrder_Fail{Fail: fail}
	}

	return &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{BackupOrder: order}}
}

func TestTechnicalAtomicity_BackupPrefixesRejected(t *testing.T) {
	t.Parallel()
	for _, incremental := range []bool{false, true} {
		for _, operation := range []string{"start", "complete", "fail"} {
			for _, failure := range []string{"backup rejection", "technical coverage", "order coverage"} {
				t.Run(map[bool]string{false: "full", true: "incremental"}[incremental]+"/"+operation+"/"+failure, func(t *testing.T) {
					t.Parallel()
					machine, store, _ := newTestMachine(t)
					seed := makeProposal(1)
					seed.AtomicTechnicalUpdates = true
					if operation != "start" {
						seed.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{technicalBackupUpdate(incremental, "start", 1, destA())}
					}
					applied, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, seed))
					require.NoError(t, err)
					require.NoError(t, applied.Results[0].Error)
					before := machine.Registry.BackupJobs.ActiveByDestination(destA())
					previous := makeProposal(2)
					previous.AtomicTechnicalUpdates = true
					previous.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{sinkCursorUpdate("preserved", 7), technicalBackupUpdate(!incremental, "start", 99, destB())}
					prefix := technicalBackupUpdate(incremental, operation, 1, destA())
					rejected := technicalCoverageRejection(3, prefix)
					if failure == "backup rejection" {
						rejected = makeProposal(3)
						rejected.AtomicTechnicalUpdates = true
						rejected.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{prefix, technicalBackupUpdate(!incremental, "fail", 999, destB())}
					}
					if failure == "order coverage" {
						rejected = technicalOrderCoverageRejection(3, prefix)
					}
					applied, err = machine.ApplyEntries(t.Context(), store,
						makeEntry(t, 2, previous), unsealedTechnicalEntry(t, 3, rejected))
					require.NoError(t, err)
					require.NoError(t, applied.Results[0].Error)
					if failure != "backup rejection" {
						requireTechnicalCoverageRejection(t, applied.Results[1].Error)
					} else {
						require.ErrorIs(t, applied.Results[1].Error, ErrBackupJobNotFound)
					}
					require.False(t, applied.Results[1].AuditEntryWritten)
					require.Equal(t, before, machine.Registry.BackupJobs.ActiveByDestination(destA()))
					require.Equal(t, uint64(99), machine.Registry.BackupJobs.ActiveByDestination(destB()).GetJobId())
					requireTechnicalCursor(t, store, "preserved", 7)
					require.Equal(t, uint64(3), machine.LastAppliedIndex())
					reader, err := store.NewReadHandle()
					require.NoError(t, err)
					history, err := dal.GetValue(reader, backupJobHistoryKey(3, CanonicalDestinationKey(destA())))
					require.NoError(t, err)
					require.Nil(t, history, "rejected lifecycle update must not append history")
					require.NoError(t, reader.Close())
					recovered := recoverMachineOnStore(t, store)
					require.NoError(t, NewRecovery(recovered, store).RestoreCacheFromStore())
					require.Equal(t, machine.Registry.BackupJobs.Snapshot(), recovered.Registry.BackupJobs.Snapshot())
					require.Equal(t, uint64(3), recovered.LastAppliedIndex())
				})
			}
		}
	}
}

func TestTechnicalAtomicity_BackupLifecycleSequencesSucceed(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	first := makeProposal(1)
	first.AtomicTechnicalUpdates = true
	first.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
		technicalBackupUpdate(false, "start", 1, destA()),
		technicalBackupUpdate(false, "complete", 1, destA()),
		technicalBackupUpdate(true, "start", 2, destA()),
	}
	second := makeProposal(2)
	second.AtomicTechnicalUpdates = true
	second.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{
		technicalBackupUpdate(true, "fail", 2, destA()),
		technicalBackupUpdate(false, "start", 3, destA()),
	}
	applied, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, first), makeEntry(t, 2, second))
	require.NoError(t, err)
	for _, result := range applied.Results {
		require.NoError(t, result.Error)
	}
	active := machine.Registry.BackupJobs.ActiveByDestination(destA())
	require.Equal(t, uint64(3), active.GetJobId())
	require.Equal(t, raftcmdpb.BackupKind_BACKUP_KIND_FULL, active.GetKind())
	require.Equal(t, uint64(2), active.GetStartedAtAppliedIndex())
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	complete, err := dal.ReadProto[*raftcmdpb.BackupJob](reader, backupJobHistoryKey(1, CanonicalDestinationKey(destA())))
	require.NoError(t, err)
	require.Equal(t, uint64(1), complete.GetJobId())
	require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_COMPLETE, complete.GetStatus())
	require.Equal(t, uint64(45), complete.GetFilesUploaded())
	failed, err := dal.ReadProto[*raftcmdpb.BackupJob](reader, backupJobHistoryKey(2, CanonicalDestinationKey(destA())))
	require.NoError(t, err)
	require.Equal(t, uint64(2), failed.GetJobId())
	require.Equal(t, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, failed.GetKind())
	require.Equal(t, raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_FAILED, failed.GetStatus())
	require.Equal(t, "upload failed", failed.GetFailureMessage())
	recovered := recoverMachineOnStore(t, store)
	require.NoError(t, NewRecovery(recovered, store).RestoreCacheFromStore())
	require.Equal(t, machine.Registry.BackupJobs.Snapshot(), recovered.Registry.BackupJobs.Snapshot())
	require.Equal(t, uint64(2), recovered.LastAppliedIndex())
}
