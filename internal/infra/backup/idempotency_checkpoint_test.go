package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/auditindexer"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestBackup_IdempotencyExpiresAtSecondCheckpointParity(t *testing.T) {
	t.Parallel()
	const (
		bucketID  = "idempotency-expiry"
		key       = "idem-restore-key"
		createdAt = uint64(1_000_000)
		expiresAt = uint64(61_000_000)
	)
	src := newBackupTestStore(t)
	seedBackupTestAuditKey(t, src)
	storage := newInMemoryBackupStorage()
	pre := src.OpenWriteSession()
	require.NoError(t, pre.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, pre.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, pre.Commit())
	_, err := RunBackup(t.Context(), logging.Testing(), src, storage, bucketID, "first")
	require.NoError(t, err)

	post := src.OpenWriteSession()
	require.NoError(t, post.SetProto(coldLogKey(2), createLedgerLog(2, "ledger", 2)))
	require.NoError(t, post.SetProto(coldAuditKey(2), &auditpb.AuditEntry{
		Sequence: 2, Timestamp: &commonpb.Timestamp{Data: createdAt},
		Idempotency: &commonpb.Idempotency{Key: key, ExpiresAt: expiresAt},
		Outcome:     &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 2, MaxLogSequence: 2}},
	}))
	require.NoError(t, state.SaveIdempotencyKey(post, key, &commonpb.IdempotencyKeyValue{
		FirstLogSequence: 2, LogCount: 1, CreatedAt: createdAt, ExpiresAt: expiresAt,
	}))
	require.NoError(t, post.Commit())
	second, err := RunBackup(t.Context(), logging.Testing(), src, storage, bucketID, "second")
	require.NoError(t, err)
	require.Equal(t, uint64(2), second.LastAuditSequence)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.Empty(t, manifest.Exports)
	dst := restoreAuditKeyCheckpoint(t, storage, manifest)

	srcH, err := src.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, srcH.Close()) }()
	dstH, err := dst.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, dstH.Close()) }()
	srcRow, err := state.LoadIdempotencyKey(srcH, key)
	require.NoError(t, err)
	dstRow, err := state.LoadIdempotencyKey(dstH, key)
	require.NoError(t, err)
	require.NotNil(t, dstRow)
	require.Equal(t, srcRow, dstRow)
	require.Equal(t, expiresAt, dstRow.GetExpiresAt())
	srcHashes, _, err := state.NewIdempotencyStore().ScanExpiredKeyHashes(srcH, expiresAt, 100)
	require.NoError(t, err)
	dstHashes, _, err := state.NewIdempotencyStore().ScanExpiredKeyHashes(dstH, expiresAt, 100)
	require.NoError(t, err)
	require.Len(t, dstHashes, 1)
	require.Equal(t, srcHashes, dstHashes)

	rs, err := readstore.New(t.TempDir(), logging.Testing(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rs.Close()) })
	idx := auditindexer.New(auditindexer.Config{}, dst, rs, logging.Testing(), noop.NewMeterProvider().Meter("test"))
	require.NoError(t, idx.Rebuild(t.Context()))
	seqs, err := rs.AuditSeqsByString(readstore.AuditFieldIdempotencyKey, key)
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, seqs)
}
