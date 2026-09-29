package backup

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func restoreAuditKeyCheckpoint(t *testing.T, storage Storage, manifest *Manifest) *dal.Store {
	t.Helper()
	dir := t.TempDir()
	for name, file := range manifest.Checkpoint.Files {
		body, err := storage.GetFile(context.Background(), file.Key)
		require.NoError(t, err)
		data, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, data, 0o600))
	}
	store, err := dal.OpenDirect(dir, testLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, ApplyExportsAndRebuild(context.Background(), testLogger(), storage, store, manifest))

	return store
}

func auditKeyCheckerClean(t *testing.T, store *dal.Store) {
	t.Helper()
	var findings []*servicepb.CheckStoreError
	checker := check.NewChecker(store, attributes.New(), nil, testLogger())
	require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			findings = append(findings, e.Error)
		}
	}))
	require.Empty(t, findings)
}

func recoveredAuditKeyMachine(t *testing.T, store *dal.Store) *state.Machine {
	t.Helper()
	meter := noop.NewMeterProvider()
	c, err := cache.New(1000, meter.Meter("test"))
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attributes.New())
	m, err := state.NewMachine(testLogger(), registry, state.NewCacheSnapshotter(testLogger(), registry, nil), store,
		dal.NewSentinelFactory(store, false), meter, keystore.NewKeyStore(), state.NewSharedState(),
		signingParityNotifier{}, nil, "", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	require.NoError(t, state.NewRecovery(m, store).RecoverState())

	return m
}

func lastAuditForKeyTest(t *testing.T, store *dal.Store) *auditpb.AuditEntry {
	t.Helper()
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	entry, err := query.ReadLastAuditEntry(handle)
	require.NoError(t, err)

	return entry
}

func TestAuditKeyFullIncrementalRestoreAcrossClusterIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage := newInMemoryBackupStorage()
	source, sourceStore, _ := newSigningParityMachine(t)
	keyA, err := query.ReadAuditKey(sourceStore)
	require.NoError(t, err)
	require.Len(t, keyA, 32)
	applySigningEntry(t, source, sourceStore, 1, signingRegisterOrder("before", make([]byte, 32), ""))
	require.NoError(t, sourceStore.Flush())
	_, err = RunBackup(ctx, testLogger(), sourceStore, storage, "cluster-a", "audit-key-full-a")
	require.NoError(t, err)
	applySigningEntry(t, source, sourceStore, 2, signingRegisterOrder("after", make([]byte, 32), ""))
	require.NoError(t, sourceStore.Flush())
	inc, err := RunIncrementalBackup(ctx, testLogger(), sourceStore, storage, "cluster-a", 0)
	require.NoError(t, err)
	require.Positive(t, inc.AuditEntriesExported)
	manifest, err := ReadManifest(ctx, storage, ManifestKey("cluster-a"))
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Exports)

	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	for seq := uint64(1); seq <= 2; seq++ {
		for _, sub := range []byte{dal.SubHistoryAudit, dal.SubHistoryAuditItem} {
			key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, sub).PutUint64(seq).Build()
			if sub == dal.SubHistoryAuditItem {
				key = append(key, 0, 0, 0, 0)
			}
			sourceBytes, err := dal.GetValue(sourceStore, key)
			require.NoError(t, err)
			restoredBytes, err := dal.GetValue(restored, key)
			require.NoError(t, err)
			require.Equal(t, sourceBytes, restoredBytes, "history and business order bytes at audit sequence %d", seq)
		}
	}
	require.NoError(t, attributes.PrepareForBackup(restored))
	config := &commonpb.PersistedConfig{NodeId: 1, ClusterId: "cluster-b", StorageSchemaVersion: 5}
	batch := restored.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}, config))
	require.NoError(t, batch.Commit())
	keyB, err := query.ReadAuditKey(restored)
	require.NoError(t, err)
	require.Equal(t, keyA, keyB)
	storedConfig, err := query.ReadPersistedConfig(restored)
	require.NoError(t, err)
	require.Equal(t, "cluster-b", storedConfig.GetClusterId())
	auditKeyCheckerClean(t, restored)

	// The destination's new Raft log starts at the checkpoint boundary + 1;
	// the delta's audit sequence is already present and becomes the hash head.
	destination := recoveredAuditKeyMachine(t, restored)
	require.Equal(t, string(keyA), destination.State.AuditKey)
	before := lastAuditForKeyTest(t, restored)
	require.Equal(t, uint64(2), before.GetSequence())
	applySigningEntry(t, destination, restored, destination.State.LastAppliedIndex+1,
		signingRegisterOrder("new-on-b", make([]byte, 32), ""))
	after := lastAuditForKeyTest(t, restored)
	require.Equal(t, uint64(3), after.GetSequence())
	require.NotEqual(t, before.GetHash(), after.GetHash())
	auditKeyCheckerClean(t, restored)

	require.NoError(t, restored.Flush())
	_, err = RunBackup(ctx, testLogger(), restored, storage, "cluster-b", "audit-key-full-b")
	require.NoError(t, err)
	bManifest, err := ReadManifest(ctx, storage, ManifestKey("cluster-b"))
	require.NoError(t, err)
	require.NotNil(t, bManifest.Checkpoint)
	third := restoreAuditKeyCheckpoint(t, storage, bManifest)
	keyC, err := query.ReadAuditKey(third)
	require.NoError(t, err)
	require.Equal(t, keyA, keyC, "B's backup must preserve A's audit key")
	thirdConfig, err := query.ReadPersistedConfig(third)
	require.NoError(t, err)
	require.Equal(t, "cluster-b", thirdConfig.GetClusterId())
	auditKeyCheckerClean(t, third)
}
