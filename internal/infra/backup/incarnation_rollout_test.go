package backup

import (
	"math/big"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/incarnationmigration"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

func TestDormantCheckpointRestoreCanCommitLegacyBusinessWrite(t *testing.T) {
	ctx := logging.TestingContext()
	meters := noop.NewMeterProvider()
	store, err := dal.NewStore(filepath.Join(t.TempDir(), "source"), logging.Testing(), meters.Meter("legacy.restore"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	seedBackupTestAuditKey(t, store)
	source := newScriptedNode(t, store)
	source.applyClusterPolicy(&commonpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	})
	source.apply(actions.CreateLedgerAction("before-restore", nil))
	storage := newInMemoryBackupStorage()
	_, err = RunBackup(ctx, logging.Testing(), store, storage, "legacy", "checkpoint")
	require.NoError(t, err)
	manifest, err := ReadManifest(ctx, storage, ManifestKey("legacy"))
	require.NoError(t, err)
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	require.NoError(t, incarnationmigration.MigrateBeforeRaft(ctx, restored))
	marker, err := incarnationmigration.ReadMarker(restored)
	require.NoError(t, err)
	require.False(t, marker.Complete)
	ids, err := incarnationmigration.AdvertisedIDs(restored)
	require.NoError(t, err)
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1)}, ids)
	restarted := newScriptedNode(t, restored)
	restarted.apply(actions.CreateLedgerAction("after-restore", nil))
	ledger, err := query.GetLedgerByName(ctx, restored, "after-restore")
	require.NoError(t, err)
	require.Equal(t, "after-restore", ledger.GetName())
}

// The scripted node applies real admission proposals through the real FSM. Only
// Raft transport and the cluster membership gate are replaced by the harness.
func TestAccountIncarnationRolloutAcrossOfflineMigration(t *testing.T) {
	const ledger = "incarnation-rollout"
	ctx := logging.TestingContext()
	dir := filepath.Join(t.TempDir(), "node")
	meters := noop.NewMeterProvider()
	store, err := dal.NewStore(dir, logging.Testing(), meters.Meter("rollout"), dal.DefaultConfig())
	require.NoError(t, err)
	seedBackupTestAuditKey(t, store)

	node := newScriptedNode(t, store)
	policy := &commonpb.ClusterPolicy{
		Revision:                    1,
		QueryCheckpointLimit:        10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	}
	node.applyClusterPolicy(policy)
	node.apply(actions.CreateLedgerAction(ledger, nil))
	posting := func(source, destination string) *commonpb.Posting {
		return actions.NewPosting(source, destination, big.NewInt(10), "USD")
	}
	node.apply(actions.CreateTransactionAction(ledger, []*commonpb.Posting{posting("world", "alice")}, nil, nil))
	node.apply(actions.SaveAccountMetadataAction(ledger, "metadata-only", map[string]string{"owner": "alice"}))
	readAccount := func(s *dal.Store, address string) *commonpb.Account {
		t.Helper()
		controller := ctrl.NewDefaultController(nil, s, logging.Testing(), attributes.New(), nil, nil, meters.Meter("rollout.read"))
		account, readErr := controller.GetAccount(ctx, ledger, address, ctrl.GetAccountOptions{})
		require.NoError(t, readErr)

		return account
	}
	attr := attributes.New().AccountIncarnation
	key := (domain.AccountKey{LedgerName: ledger, Account: "alice"}).Bytes()
	before, err := attr.Get(store, key)
	require.NoError(t, err)
	require.Nil(t, before)
	require.Nil(t, readAccount(store, "alice").GetInsertionDate())

	writes := policy.CloneVT()
	writes.Revision++
	writes.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}}
	node.applyClusterPolicy(writes)
	node.activateCapability(capabilities.AccountIncarnationWritesV1)
	node.apply(actions.CreateTransactionAction(ledger, []*commonpb.Posting{posting("world", "alice")}, nil, nil))
	node.apply(actions.CreateTransactionAction(ledger, []*commonpb.Posting{posting("alice", "bob")}, nil, nil))
	live, err := attr.Get(store, key)
	require.NoError(t, err)
	require.NotNil(t, live)
	require.Greater(t, live.GetUpdatedAt().GetData(), live.GetInsertedAt().GetData())
	require.Nil(t, readAccount(store, "alice").GetInsertionDate(), "dates stay hidden before history activation")
	require.ErrorContains(t, incarnationmigration.RequireReady(store), "migrate-incarnations")
	require.NoError(t, store.Close())

	store, err = dal.NewStore(dir, logging.Testing(), meters.Meter("rollout.reopened"), dal.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.ErrorContains(t, incarnationmigration.RequireReady(store), "migrate-incarnations")
	require.NoError(t, incarnationmigration.Run(ctx, store))
	require.NoError(t, incarnationmigration.Run(ctx, store), "the offline backfill must be idempotent")
	require.NoError(t, incarnationmigration.RequireReady(store))
	marker, err := incarnationmigration.ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1), uint32(capabilities.AccountIncarnationHistoryCompleteV1)}, mustAdvertisedIDs(t, store))
	merged, err := attr.Get(store, key)
	require.NoError(t, err)
	require.Less(t, merged.GetInsertedAt().GetData(), live.GetInsertedAt().GetData())
	require.Equal(t, live.GetUpdatedAt().GetData(), merged.GetUpdatedAt().GetData())
	require.Nil(t, readAccount(store, "alice").GetInsertionDate())

	restarted := newScriptedNode(t, store)
	history := writes.CloneVT()
	history.Revision++
	history.CapabilityActivations = append(history.CapabilityActivations,
		&commonpb.CapabilityActivation{Id: uint32(capabilities.AccountIncarnationHistoryCompleteV1), Active: true})
	restarted.applyClusterPolicy(history)
	restarted.activateCapability(capabilities.AccountIncarnationHistoryCompleteV1)
	account := readAccount(store, "alice")
	require.Equal(t, merged.GetInsertedAt().GetData(), account.GetInsertionDate().GetData())
	require.Equal(t, merged.GetUpdatedAt().GetData(), account.GetUpdatedAt().GetData())
	metadataOnly := readAccount(store, "metadata-only")
	require.NotNil(t, metadataOnly.GetInsertionDate())
	require.Equal(t, metadataOnly.GetInsertionDate().GetData(), metadataOnly.GetUpdatedAt().GetData())
	storage := newInMemoryBackupStorage()
	_, err = RunBackup(ctx, logging.Testing(), store, storage, "incarnations", "before-update")
	require.NoError(t, err)
	restarted.apply(actions.CreateTransactionAction(ledger, []*commonpb.Posting{posting("world", "alice")}, nil, nil))
	updated := readAccount(store, "alice")
	require.Equal(t, account.GetInsertionDate().GetData(), updated.GetInsertionDate().GetData())
	require.Greater(t, updated.GetUpdatedAt().GetData(), account.GetUpdatedAt().GetData())
	_, err = RunBackup(ctx, logging.Testing(), store, storage, "incarnations", "after-update")
	require.NoError(t, err)
	manifest, err := ReadManifest(ctx, storage, ManifestKey("incarnations"))
	require.NoError(t, err)
	require.Empty(t, manifest.Exports)
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	require.NoError(t, incarnationmigration.RequireReady(restored))
	restoredMarker, err := incarnationmigration.ReadMarker(restored)
	require.NoError(t, err)
	require.Equal(t, marker, restoredMarker)
	restoredRow, err := attr.Get(restored, key)
	require.NoError(t, err)
	require.Equal(t, updated.GetInsertionDate().GetData(), restoredRow.GetInsertedAt().GetData())
	require.Equal(t, updated.GetUpdatedAt().GetData(), restoredRow.GetUpdatedAt().GetData())
	restoredAccount := readAccount(restored, "alice")
	require.Equal(t, updated.GetInsertionDate().GetData(), restoredAccount.GetInsertionDate().GetData())
	require.Equal(t, updated.GetUpdatedAt().GetData(), restoredAccount.GetUpdatedAt().GetData())
	auditKeyCheckerClean(t, restored)
}

func mustAdvertisedIDs(t *testing.T, store *dal.Store) []uint32 {
	t.Helper()
	ids, err := incarnationmigration.AdvertisedIDs(store)
	require.NoError(t, err)

	return ids
}
