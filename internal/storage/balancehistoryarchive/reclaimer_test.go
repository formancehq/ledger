package balancehistoryarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

func TestOwnedArchiveReclaimerPaginatesAndFiltersForeignObjects(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	backend := coldstorage.NewFilesystemStorage(root)
	ownerConfig := Config{
		BaseBucketID:  "cluster",
		OwnerID:       "node-1",
		CacheDir:      t.TempDir(),
		CacheMaxBytes: 8 << 20,
	}
	store, err := New(backend, ownerConfig, noop.NewMeterProvider().Meter("owner-reclaimer-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	var want []Ref
	for _, seed := range []string{"owner-a", "owner-b", "owner-c"} {
		ref, err := store.Archive(context.Background(), NewSliceStream(testRecords(seed, 32)))
		require.NoError(t, err)
		want = append(want, ref)
	}

	foreignConfig := ownerConfig
	foreignConfig.OwnerID = "node-2"
	foreignConfig.CacheDir = t.TempDir()
	foreign, err := New(backend, foreignConfig, noop.NewMeterProvider().Meter("foreign-reclaimer-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, foreign.Close()) })
	foreignRef, err := foreign.Archive(context.Background(), NewSliceStream(testRecords("foreign", 32)))
	require.NoError(t, err)

	ownerPrefix := "cluster/balance-history/nodes/node-1/runs"
	malformedBucket := ownerPrefix + "/not-a-sha256"
	malformedBody := []byte("malformed")
	require.NoError(t, backend.Archive(
		context.Background(),
		malformedBucket,
		archiveChapterID,
		bytes.NewReader(malformedBody),
		checksum(malformedBody),
	))
	nonzeroChapterBucket, err := ownerConfig.ObjectBucketID(want[0])
	require.NoError(t, err)
	require.NoError(t, backend.Archive(
		context.Background(),
		nonzeroChapterBucket,
		1,
		strings.NewReader("wrong chapter"),
		checksum([]byte("wrong chapter")),
	))

	var (
		cursor string
		got    []RemoteObject
	)
	for {
		page, err := store.List(context.Background(), cursor, 1)
		require.NoError(t, err)
		got = append(got, page.Objects...)
		if page.NextCursor == "" {
			break
		}
		require.NotEqual(t, cursor, page.NextCursor)
		cursor = page.NextCursor
	}
	require.Len(t, got, len(want))
	gotDigests := make(map[[sha256.Size]byte]struct{}, len(got))
	for _, object := range got {
		gotDigests[object.SHA256] = struct{}{}
		require.Positive(t, object.Size)
		require.False(t, object.LastModified.IsZero())
	}
	for _, ref := range want {
		_, ok := gotDigests[ref.SHA256]
		require.True(t, ok)
	}
	_, ok := gotDigests[foreignRef.SHA256]
	require.False(t, ok, "another stable owner namespace must never be enumerated")
}

func TestOwnedArchiveReclaimerDeleteIsIdempotentAndOwnerScoped(t *testing.T) {
	t.Parallel()

	backend := coldstorage.NewFilesystemStorage(t.TempDir())
	ownerConfig := Config{BaseBucketID: "cluster", OwnerID: "node-1", CacheDir: t.TempDir(), CacheMaxBytes: 1 << 20}
	owner, err := New(backend, ownerConfig, noop.NewMeterProvider().Meter("owner-delete-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	foreignConfig := Config{BaseBucketID: "cluster", OwnerID: "node-2", CacheDir: t.TempDir(), CacheMaxBytes: 1 << 20}
	foreign, err := New(backend, foreignConfig, noop.NewMeterProvider().Meter("foreign-delete-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, foreign.Close()) })

	records := testRecords("shared-content", 64)
	ownerRef, err := owner.Archive(context.Background(), NewSliceStream(records))
	require.NoError(t, err)
	foreignRef, err := foreign.Archive(context.Background(), NewSliceStream(records))
	require.NoError(t, err)
	require.Equal(t, ownerRef, foreignRef)

	require.NoError(t, owner.Delete(context.Background(), ownerRef.SHA256))
	require.NoError(t, owner.Delete(context.Background(), ownerRef.SHA256), "delete must be idempotent")
	ownerBucket, err := ownerConfig.ObjectBucketID(ownerRef)
	require.NoError(t, err)
	exists, err := backend.Exists(context.Background(), ownerBucket, archiveChapterID)
	require.NoError(t, err)
	require.False(t, exists)
	foreignBucket, err := foreignConfig.ObjectBucketID(foreignRef)
	require.NoError(t, err)
	exists, err = backend.Exists(context.Background(), foreignBucket, archiveChapterID)
	require.NoError(t, err)
	require.True(t, exists, "deleting one owner must not delete identical content owned by another node")
	require.Error(t, owner.Delete(context.Background(), [sha256.Size]byte{}))
}

func TestOwnedArchiveReclaimerRequiresCatalog(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	store := newTestStore(t, cold, t.TempDir(), 1<<20)
	_, err := store.List(context.Background(), "", 1)
	require.ErrorIs(t, err, ErrReclamationUnsupported)
	require.ErrorIs(t, store.Delete(context.Background(), sha256.Sum256([]byte("missing"))), ErrReclamationUnsupported)
}

func TestArchiveOwnerNamespaceValidation(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	for _, ownerID := range []string{"", "node/other", `node\othere`, ".", "..", " node-1"} {
		_, err := New(cold.mock, Config{
			BaseBucketID:  "cluster",
			OwnerID:       ownerID,
			CacheDir:      t.TempDir(),
			CacheMaxBytes: 1 << 20,
		}, noop.NewMeterProvider().Meter("invalid-owner-test"))
		require.Error(t, err, "owner %q must be rejected", ownerID)
	}
}

func checksum(data []byte) []byte {
	digest := sha256.Sum256(data)

	return digest[:]
}
