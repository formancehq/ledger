package backup

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func kvOrder(ledger, key string, value []byte) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger, Payload: &raftcmdpb.LedgerScopedOrder_PutKv{PutKv: &raftcmdpb.PutKVOrder{Key: key, Value: value}},
	}}}
}

func deleteKVOrder(ledger, key string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger, Payload: &raftcmdpb.LedgerScopedOrder_DeleteKv{DeleteKv: &raftcmdpb.DeleteKVOrder{Key: key}},
	}}}
}

func readKVRow(t *testing.T, store *dal.Store, ledger, key string) []byte {
	t.Helper()
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	value, closer, err := handle.Get(dal.LedgerKVKey(ledger, key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	require.NoError(t, err)
	defer func() { require.NoError(t, closer.Close()) }()

	return bytes.Clone(value)
}

func TestBackup_LedgerKVRestoreParity(t *testing.T) {
	t.Parallel()
	const ledger = "kv-restore"
	const bucket = "kv-restore-bucket"

	source, store, attrs := newSigningParityMachine(t)
	require.NoError(t, applyMetadataCreation(t, source, store, 1,
		metadataCreationOrder(ledger, commonpb.LedgerMode_LEDGER_MODE_NORMAL, nil))[0].Error)
	require.NoError(t, applyMetadataCreation(t, source, store, 2,
		kvOrder(ledger, "remove", []byte("checkpoint")))[0].Error)
	require.NoError(t, store.Flush())

	storage := newInMemoryBackupStorage()
	full, err := RunBackup(t.Context(), testLogger(), store, storage, bucket, "full")
	require.NoError(t, err)
	require.Positive(t, full.LastLogSequence)
	dstDir := t.TempDir()
	require.NoError(t, store.Checkpoint(filepath.Join(dstDir, "live")))

	require.NoError(t, applyMetadataCreation(t, source, store, 3,
		kvOrder(ledger, "binary", []byte{0, 255, 1}))[0].Error)
	require.NoError(t, applyMetadataCreation(t, source, store, 4,
		deleteKVOrder(ledger, "remove"))[0].Error)
	require.NoError(t, store.Flush())
	inc, err := RunIncrementalBackup(t.Context(), testLogger(), store, storage, bucket, 0)
	require.NoError(t, err)
	require.Positive(t, inc.LogEntriesExported)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey(bucket))
	require.NoError(t, err)

	dst, err := dal.NewStore(dstDir, testLogger(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dst.Close()) })
	require.NoError(t, ApplyExportsAndRebuild(t.Context(), testLogger(), storage, dst, manifest))
	require.Equal(t, []byte{0, 255, 1}, readKVRow(t, dst, ledger, "binary"))
	require.Nil(t, readKVRow(t, dst, ledger, "remove"))
	require.Equal(t, readKVRow(t, store, ledger, "binary"), readKVRow(t, dst, ledger, "binary"))
	require.Empty(t, creationIntegrityFindings(t, dst, attributes.New()))
	require.Empty(t, creationIntegrityFindings(t, store, attrs))
}
