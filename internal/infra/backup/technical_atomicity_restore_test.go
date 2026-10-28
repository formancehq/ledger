package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestBackup_TechnicalAtomicityRestoreParity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage := newInMemoryBackupStorage()
	source, sourceStore, _ := newSigningParityMachine(t)

	apply := func(machine *state.Machine, store *dal.Store, p *raftcmdpb.Proposal) state.ApplyResult {
		t.Helper()
		index := machine.State.LastAppliedIndex + 1
		p.Id = index
		data, err := proto.Marshal(p)
		require.NoError(t, err)
		result, err := machine.ApplyEntries(ctx, store, &raftpb.Entry{
			Index: new(index), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data,
		})
		require.NoError(t, err)
		require.Len(t, result.Results, 1)

		return result.Results[0]
	}
	backupStart := func(jobID uint64) *raftcmdpb.TechnicalUpdate {
		return &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{
			BackupOrder: &raftcmdpb.BackupOrder{Op: &raftcmdpb.BackupOrder_Start{
				Start: &raftcmdpb.BackupOrderStart{JobId: jobID, Destination: &raftcmdpb.BackupDestination{
					BucketId: "atomicity-restore",
					Target:   &raftcmdpb.BackupDestination_S3{S3: &raftcmdpb.S3BackupTarget{Bucket: "backups"}},
				}},
			}},
		}}
	}
	proposal := func(key, signingID string, date uint64, jobID uint64) *raftcmdpb.Proposal {
		return &raftcmdpb.Proposal{
			AtomicTechnicalUpdates: true,
			Orders:                 []*raftcmdpb.Order{signingRegisterOrder(signingID, make([]byte, 32), "")},
			TechnicalUpdates:       []*raftcmdpb.TechnicalUpdate{backupStart(jobID)},
			Date:                   &commonpb.Timestamp{Data: date},
			Idempotency:            &commonpb.Idempotency{Key: key},
			ExecutionPlan:          &raftcmdpb.ExecutionPlan{},
			CallerSnapshot:         commands.SystemCallerSnapshot(commands.ComponentClusterConfig),
		}
	}
	frozen := func(store *dal.Store, key string) *commonpb.IdempotencyKeyValue {
		t.Helper()
		reader, err := store.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()
		v, err := state.LoadIdempotencyKey(reader, key)
		require.NoError(t, err)

		return v
	}

	// A real non-empty checkpoint seeds both the key registry and audit chain.
	applySigningEntry(t, source, sourceStore, 1, signingRegisterOrder("before", make([]byte, 32), ""))
	require.NoError(t, sourceStore.Flush())
	_, err := RunBackup(ctx, testLogger(), sourceStore, storage, "atomicity-source", "full")
	require.NoError(t, err)
	checkpointTime := source.State.LastAppliedTimestamp

	// A valid first update followed by a missing-job rejection drops the entire
	// technical prefix before the proposed business order ever executes.
	technicalRejected := proposal("technical-rejection", "never", checkpointTime+500, 5)
	technicalRejected.TechnicalUpdates = append(technicalRejected.TechnicalUpdates,
		&raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{
			BackupOrder: &raftcmdpb.BackupOrder{Op: &raftcmdpb.BackupOrder_Fail{
				Fail: &raftcmdpb.BackupOrderFail{JobId: 999},
			}},
		}})
	technicalResult := apply(source, sourceStore, technicalRejected)
	require.ErrorIs(t, technicalResult.Error, state.ErrBackupJobNotFound)
	require.False(t, technicalResult.AuditEntryWritten)
	require.Nil(t, frozen(sourceStore, "technical-rejection"))
	require.Empty(t, source.Registry.BackupJobs.Snapshot())
	require.Equal(t, checkpointTime, source.State.LastAppliedTimestamp)

	// The prepared backup must not survive a definitive rejection, although the
	// rejected business proposal still contributes a failure audit and frozen key.
	failure := apply(source, sourceStore, proposal("definitive", "", checkpointTime+1, 10))
	require.ErrorIs(t, failure.Error, domain.ErrSigningKeyIDRequired)
	require.True(t, failure.AuditEntryWritten)
	require.Empty(t, source.Registry.BackupJobs.Snapshot())
	require.NotNil(t, frozen(sourceStore, "definitive").GetFailure())
	failureTime := source.State.LastAppliedTimestamp
	require.Equal(t, checkpointTime+1, failureTime)

	// An uncommitted first attempt creates neither audit nor frozen result. Its
	// high timestamp must not enter the exported HLC fold.
	unavailable := proposal("repairable", "repaired", checkpointTime+1000, 20)
	unavailable.Orders[0].Technical = &raftcmdpb.OrderTechnical{PreloadUnavailable: true}
	interrupted := apply(source, sourceStore, unavailable)
	require.ErrorIs(t, interrupted.Error, domain.ErrPreloadUnavailable)
	require.False(t, interrupted.AuditEntryWritten)
	require.Nil(t, frozen(sourceStore, "repairable"))
	require.Empty(t, source.Registry.BackupJobs.Snapshot())
	require.Equal(t, failureTime, source.State.LastAppliedTimestamp)

	// The successful mixed proposal supplies a rebuilt business projection and
	// a transient job. The job is deliberately discarded during restore rather
	// than invented from the exported business order.
	success := apply(source, sourceStore, proposal("accepted", "after", checkpointTime+2, 30))
	require.NoError(t, success.Error)
	require.True(t, success.AuditEntryWritten)
	require.Len(t, source.Registry.BackupJobs.Snapshot(), 1)
	require.ElementsMatch(t, []string{"before", "after"}, signingKeyIDs(t, sourceStore))
	sourceAudit := lastAuditForKeyTest(t, sourceStore).CloneVT()
	require.Equal(t, uint64(3), sourceAudit.GetSequence())

	require.NoError(t, sourceStore.Flush())
	inc, err := RunIncrementalBackup(ctx, testLogger(), sourceStore, storage, "atomicity-source", 0)
	require.NoError(t, err)
	require.Positive(t, inc.LogEntriesExported)
	require.Equal(t, int64(2), int64(inc.AuditEntriesExported), "one failure and one success; no interrupted attempt")
	manifest, err := ReadManifest(ctx, storage, ManifestKey("atomicity-source"))
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Checkpoint.Files)
	require.NotEmpty(t, manifest.Exports)

	// This helper copies the real checkpoint and invokes ApplyExportsAndRebuild.
	restoredStore := restoreAuditKeyCheckpoint(t, storage, manifest)
	require.NoError(t, attributes.PrepareForBackup(restoredStore))
	restored := recoveredAuditKeyMachine(t, restoredStore)
	require.Empty(t, restored.Registry.BackupJobs.Snapshot(), "source-cluster jobs are discarded")
	readKeys := func(store *dal.Store) map[string]query.SigningKeyEntry {
		t.Helper()
		reader, err := store.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()
		keys, malformed, err := query.ReadSigningKeys(reader)
		require.NoError(t, err)
		require.Empty(t, malformed)

		return keys
	}
	require.Equal(t, readKeys(sourceStore), readKeys(restoredStore))
	for seq := uint64(1); seq <= sourceAudit.GetSequence(); seq++ {
		for _, sub := range []byte{dal.SubHistoryAudit, dal.SubHistoryAuditItem} {
			key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, sub).PutUint64(seq).Build()
			if sub == dal.SubHistoryAuditItem {
				key = append(key, 0, 0, 0, 0)
			}
			live, err := dal.GetValue(sourceStore, key)
			require.NoError(t, err)
			got, err := dal.GetValue(restoredStore, key)
			require.NoError(t, err)
			require.Equal(t, live, got, "audit sequence %d sub-prefix %d", seq, sub)
		}
	}
	require.True(t, proto.Equal(sourceAudit, lastAuditForKeyTest(t, restoredStore)))
	require.Equal(t, source.State.LastAppliedTimestamp, restored.State.LastAppliedTimestamp)
	for _, key := range []string{"definitive", "accepted", "repairable", "technical-rejection"} {
		require.True(t, proto.Equal(frozen(sourceStore, key), frozen(restoredStore, key)), key)
	}
	for _, store := range []*dal.Store{sourceStore, restoredStore} {
		auditKeyCheckerClean(t, store)
	}

	// Retrying both retained outcomes must replay without publishing another
	// prepared job. Release the live source's successful job first so backup
	// validation cannot mask the replay branch we are testing.
	complete := proposal("", "unused", checkpointTime+3, 30)
	complete.Orders = nil
	complete.TechnicalUpdates[0] = &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{
		BackupOrder: &raftcmdpb.BackupOrder{Op: &raftcmdpb.BackupOrder_Fail{
			Fail: &raftcmdpb.BackupOrderFail{JobId: 30},
		}},
	}}
	require.NoError(t, apply(source, sourceStore, complete).Error)
	for _, pair := range []struct {
		machine *state.Machine
		store   *dal.Store
	}{{source, sourceStore}, {restored, restoredStore}} {
		beforeTime := pair.machine.State.LastAppliedTimestamp
		for _, replay := range []*raftcmdpb.Proposal{
			proposal("accepted", "after", checkpointTime+100, 40),
			proposal("definitive", "", checkpointTime+100, 50),
		} {
			result := apply(pair.machine, pair.store, replay)
			require.True(t, result.Replayed)
			require.False(t, result.AuditEntryWritten)
			if replay.GetIdempotency().GetKey() == "definitive" {
				require.Error(t, result.Error)
			} else {
				require.NoError(t, result.Error)
			}
			require.Empty(t, pair.machine.Registry.BackupJobs.Snapshot())
			require.Equal(t, beforeTime, pair.machine.State.LastAppliedTimestamp)
		}
		repaired := proposal("repairable", "repaired", checkpointTime+3, 60)
		repaired.TechnicalUpdates = nil
		result := apply(pair.machine, pair.store, repaired)
		require.NoError(t, result.Error)
		require.False(t, result.Replayed)
		require.True(t, result.AuditEntryWritten)
		require.ElementsMatch(t, []string{"before", "after", "repaired"}, signingKeyIDs(t, pair.store))
		require.NotNil(t, frozen(pair.store, "repairable"))
		auditKeyCheckerClean(t, pair.store)
	}
	require.Equal(t, source.State.LastAppliedTimestamp, restored.State.LastAppliedTimestamp)
}
