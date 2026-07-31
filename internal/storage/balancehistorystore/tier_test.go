package balancehistorystore

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

const tierTestBucket = "pit-tier-test"

type multipartTierFixture struct {
	root       string
	historyDir string
	objectDir  string
	store      *Store
	upload     *balancehistoryarchive.Store
	manifest   Manifest
}

func newMultipartTierFixture(t *testing.T, publications int) *multipartTierFixture {
	t.Helper()

	fixture := &multipartTierFixture{root: t.TempDir()}
	fixture.historyDir = filepath.Join(fixture.root, "history")
	fixture.objectDir = filepath.Join(fixture.root, "objects")
	fixture.store = openTestStoreAt(t, fixture.historyDir)
	var err error
	fixture.upload, err = balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		balancehistoryarchive.Config{
			BaseBucketID:  tierTestBucket,
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(fixture.root, "upload-cache"),
			CacheMaxBytes: 64 << 20,
		},
		noop.NewMeterProvider().Meter("balance-history-tier-upload-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if fixture.store != nil {
			require.NoError(t, fixture.store.Close())
			fixture.store = nil
		}
		require.NoError(t, fixture.upload.Close())
	})

	for sequence := uint64(1); sequence <= uint64(publications); sequence++ {
		publishBalanced(t, fixture.store, sequence, sequence, sequence*10, sequence*100, sequence)
	}
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{
		Archive:         fixture.upload,
		MaxSegmentBytes: 4 << 10,
		MaxRunsPerPass:  publications + 1,
	}))
	tiered, err := fixture.store.Tier(context.Background())
	require.NoError(t, err)
	require.Equal(t, publications, tiered)
	fixture.manifest, err = fixture.store.Manifest()
	require.NoError(t, err)
	for _, run := range fixture.manifest.Runs {
		require.True(t, run.Archived)
		require.True(t, run.LocalRemoved)
		require.Greater(t, len(run.ArchiveParts), 1)
	}

	return fixture
}

func (f *multipartTierFixture) newReadArchive(
	t *testing.T,
	backend coldstorage.ColdStorage,
	cacheName string,
) *balancehistoryarchive.Store {
	t.Helper()

	archive, err := balancehistoryarchive.New(
		backend,
		balancehistoryarchive.Config{
			BaseBucketID:  tierTestBucket,
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(f.root, cacheName),
			CacheMaxBytes: 64 << 20,
		},
		noop.NewMeterProvider().Meter("balance-history-tier-read-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })

	return archive
}

func TestColdMultipartViewFetchesOnlyIntersectingParts(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 2)
	readArchive := fixture.newReadArchive(
		t,
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		"read-cache",
	)
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: readArchive}))

	view, err := fixture.store.OpenView(2)
	require.NoError(t, err)
	for _, cold := range view.coldRuns {
		cold.mu.Lock()
		for _, part := range cold.parts {
			require.Nil(t, part.reader)
			require.Nil(t, part.lease)
		}
		cold.mu.Unlock()
	}
	require.Equal(t, 0, readArchive.CacheStats().Entries)

	assets, err := view.AggregateAll(7, AxisEffective, 20)
	require.NoError(t, err)
	require.Len(t, assets, 1)
	require.Equal(t, "3", assets[0].Input.String())
	loaded := loadedArchiveParts(view)
	require.NotEmpty(t, loaded)
	for _, run := range fixture.manifest.Runs {
		assetRanges := effectiveAssetRanges(t, run.ID)
		volumeRanges := routeRanges(run.ID, AxisEffective, scopeVolume, 7)
		for _, part := range run.ArchiveParts {
			if _, ok := loaded[part.Ref.SHA256]; !ok {
				continue
			}
			require.True(t, partMatchesAny(part, assetRanges), "loaded non-asset part %x", part.Ref.SHA256)
			require.False(t, partMatchesAny(part, volumeRanges), "asset query loaded volume part %x", part.Ref.SHA256)
		}
	}

	beforeExact := loadedArchiveParts(view)
	volumes, err := view.ReadVolumes(7, AxisEffective, 20, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.Equal(t, "3", volumes[0].Input.String())
	afterExact := loadedArchiveParts(view)
	require.Greater(t, len(afterExact), len(beforeExact))
	for _, run := range fixture.manifest.Runs {
		exactRanges := effectiveExactAccountRanges(t, run.ID, "assets:cash")
		for _, part := range run.ArchiveParts {
			if _, wasLoaded := beforeExact[part.Ref.SHA256]; wasLoaded {
				continue
			}
			if _, nowLoaded := afterExact[part.Ref.SHA256]; nowLoaded {
				require.True(t, partMatchesAny(part, exactRanges), "exact query loaded unrelated part %x", part.Ref.SHA256)
			}
		}
	}
	require.NoError(t, view.Close())
}

func TestColdMultipartMissingRequiredPartFailsSourceMissing(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 1)
	run := fixture.manifest.Runs[0]
	partIndex, part := findPart(t, run, func(part ArchivePart) bool {
		return partMatchesAny(part, effectiveAssetDataRanges(t, run.ID))
	})
	require.Positive(t, partIndex)
	require.Less(t, partIndex, len(run.ArchiveParts)-1)
	require.NoError(t, os.Remove(coldObjectPath(fixture.objectDir, part.Ref)))

	readArchive := fixture.newReadArchive(
		t,
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		"missing-cache",
	)
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: readArchive}))
	view, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	_, err = view.AggregateAll(7, AxisEffective, 10)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	require.NoError(t, view.Close())
	_, err = fixture.store.OpenView(1)
	require.ErrorAs(t, err, &missing)
	require.NoError(t, fixture.store.Close())
	fixture.store = nil
	fixture.store = openTestStoreAt(t, fixture.historyDir)
	_, err = fixture.store.OpenView(1)
	require.ErrorAs(t, err, &missing)
}

func TestColdMultipartCorruptRequiredPartQuarantinesStore(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 1)
	run := fixture.manifest.Runs[0]
	_, part := findPart(t, run, func(part ArchivePart) bool {
		return partMatchesAny(part, effectiveAssetDataRanges(t, run.ID))
	})
	tamperFile(t, coldObjectPath(fixture.objectDir, part.Ref))

	readArchive := fixture.newReadArchive(
		t,
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		"corrupt-cache",
	)
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: readArchive}))
	view, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	_, err = view.AggregateAll(7, AxisEffective, 10)
	var corrupt *ErrCorrupt
	require.ErrorAs(t, err, &corrupt)
	require.NoError(t, view.Close())
	_, err = fixture.store.OpenView(1)
	var quarantined *ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
}

func TestColdMultipartRestartBeforeArchiveInjection(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 2)
	require.NoError(t, fixture.store.SyncWAL())
	require.NoError(t, fixture.store.DB().Flush())
	require.NoError(t, fixture.store.Close())
	fixture.store = nil

	reopened := openTestStoreAt(t, fixture.historyDir)
	fixture.store = reopened
	_, err := reopened.OpenView(2)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)

	readArchive := fixture.newReadArchive(
		t,
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		"restart-cache",
	)
	require.NoError(t, reopened.ConfigureTiering(TieringConfig{Archive: readArchive}))
	view, err := reopened.OpenView(2)
	require.NoError(t, err)
	assets, err := view.AggregateAll(7, AxisEffective, 20)
	require.NoError(t, err)
	require.Equal(t, "3", assets[0].Input.String())
	require.NoError(t, view.Close())
}

func TestColdFetchHonorsViewContextCancellation(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 1)
	filesystem := coldstorage.NewFilesystemStorage(fixture.objectDir)
	mock := NewMockIdentifiedStorage(gomock.NewController(t))
	physicalIdentity, err := filesystem.DestinationIdentity()
	require.NoError(t, err)
	mock.EXPECT().DestinationIdentity().AnyTimes().Return(physicalIdentity, nil)
	mock.EXPECT().Exists(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(filesystem.Exists).AnyTimes()
	mock.EXPECT().ExpectedChecksum(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(filesystem.ExpectedChecksum).AnyTimes()
	fetchStarted := make(chan struct{})
	var once sync.Once
	mock.EXPECT().Fetch(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ string, _ uint64) (io.ReadCloser, error) {
			once.Do(func() { close(fetchStarted) })
			<-ctx.Done()

			return nil, ctx.Err()
		},
	).AnyTimes()
	readArchive := fixture.newReadArchive(t, mock, "cancel-cache")
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: readArchive}))

	ctx, cancel := context.WithCancel(context.Background())
	view, err := fixture.store.OpenViewContext(ctx, 1)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, queryErr := view.AggregateAll(7, AxisEffective, 10)
		result <- queryErr
	}()
	select {
	case <-fetchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("cold fetch did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cold fetch ignored view context cancellation")
	}
	require.NoError(t, view.Close())
}

func TestColdCachedReaderChecksCancellationInsideCatalogLoop(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 1)
	readArchive := fixture.newReadArchive(
		t,
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		"cached-cancel-cache",
	)
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: readArchive}))
	prewarm, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	_, err = prewarm.ReadVolumes(7, AxisEffective, 10, []string{"assets:cash"})
	require.NoError(t, err)
	require.NoError(t, prewarm.Close())
	cacheBefore := readArchive.CacheStats()
	require.Positive(t, cacheBefore.Entries)

	mockContext := NewMockContext(gomock.NewController(t))
	var checks atomic.Int64
	mockContext.EXPECT().Err().DoAndReturn(func() error {
		if checks.Add(1) >= 8 {
			return context.Canceled
		}

		return nil
	}).AnyTimes()
	view, err := fixture.store.OpenViewContext(mockContext, 1)
	require.NoError(t, err)
	_, err = view.ReadVolumes(7, AxisEffective, 10, []string{"assets:cash"})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, cacheBefore.Entries, readArchive.CacheStats().Entries)
	require.NoError(t, view.Close())
}

func TestTierDefersLocalRemovalForPinnedViewAndGC(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for sequence := uint64(1); sequence <= 2; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence*10, sequence*100, sequence)
	}
	pinned, err := store.OpenView(2)
	require.NoError(t, err)
	archive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID:  tierTestBucket,
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(root, "cache"),
			CacheMaxBytes: 64 << 20,
		},
		noop.NewMeterProvider().Meter("balance-history-tier-lease-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archive, MaxSegmentBytes: 4 << 10}))

	tiered, err := store.Tier(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, tiered)
	manifest, err := store.Manifest()
	require.NoError(t, err)
	for _, run := range manifest.Runs {
		require.True(t, run.Archived)
		require.False(t, run.LocalRemoved)
	}
	_, err = store.CollectGarbage()
	require.NoError(t, err)
	volumes, err := pinned.ReadVolumes(7, AxisEffective, 20, []string{"assets:cash"})
	require.NoError(t, err)
	require.Equal(t, "3", volumes[0].Input.String())
	require.NoError(t, pinned.Close())

	tiered, err = store.Tier(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, tiered)
	manifest, err = store.Manifest()
	require.NoError(t, err)
	for _, run := range manifest.Runs {
		require.True(t, run.LocalRemoved)
	}
	coldView, err := store.OpenView(2)
	require.NoError(t, err)
	volumes, err = coldView.ReadVolumes(7, AxisEffective, 20, []string{"assets:cash"})
	require.NoError(t, err)
	require.Equal(t, "3", volumes[0].Input.String())
	require.NoError(t, coldView.Close())
}

func TestVerifyBoundedContextLimitsColdPartsAndReportsBytes(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 2)
	readArchive := fixture.newReadArchive(
		t,
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		"bounded-verify-cache",
	)
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: readArchive}))
	totalParts := len(flattenVerificationTargets(fixture.manifest.Runs))
	require.Greater(t, totalParts, 2)

	stats, err := fixture.store.VerifyBoundedContext(context.Background(), 0, 2)
	require.NoError(t, err)
	require.Equal(t, fixture.manifest.Version, stats.ManifestVersion)
	require.Equal(t, 2, stats.ArchiveParts)
	require.Positive(t, stats.ArchiveBytes)
	require.Equal(t, uint64(2), stats.NextOffset)
	require.False(t, stats.Complete)
	require.Equal(t, 2, readArchive.CacheStats().Entries)
}

func TestVerifyContextDetectsCoherentCatalogAndBoundsTampering(t *testing.T) {
	t.Parallel()

	t.Run("catalog content", func(t *testing.T) {
		fixture := newMultipartTierFixture(t, 1)
		manifest := cloneManifest(fixture.manifest)
		run := &manifest.Runs[0]
		partIndex, part := findPart(t, *run, func(part ArchivePart) bool {
			return len(part.LowerBound) > 0 && part.LowerBound[0] == prefixRunCatalog
		})
		records := readArchiveRecords(t, fixture.upload, part.Ref)
		require.NotEmpty(t, records)
		require.Equal(t, prefixRunCatalog, records[0].Key[0])
		records[0].Value = []byte{1}
		tamperedRef, err := fixture.upload.Archive(
			context.Background(),
			balancehistoryarchive.NewSliceStream(records),
		)
		require.NoError(t, err)
		run.ArchiveParts[partIndex].Ref = tamperedRef
		rewriteCurrentManifest(t, fixture.store, manifest)

		err = fixture.store.VerifyContext(context.Background())
		var corrupt *ErrCorrupt
		require.ErrorAs(t, err, &corrupt)
	})

	t.Run("part bounds", func(t *testing.T) {
		fixture := newMultipartTierFixture(t, 1)
		manifest := cloneManifest(fixture.manifest)
		run := &manifest.Runs[0]
		partIndex := -1
		for index := 1; index < len(run.ArchiveParts); index++ {
			records := readArchiveRecords(t, fixture.upload, run.ArchiveParts[index].Ref)
			if len(records) == 0 || bytes.Equal(records[0].Key, run.ArchiveParts[index].LowerBound) {
				continue
			}
			partIndex = index

			break
		}
		require.Positive(t, partIndex)
		newBoundary := append(bytes.Clone(run.ArchiveParts[partIndex].LowerBound), 0)
		require.Less(t, bytes.Compare(newBoundary, run.ArchiveParts[partIndex].UpperBound), 0)
		run.ArchiveParts[partIndex-1].UpperBound = bytes.Clone(newBoundary)
		run.ArchiveParts[partIndex].LowerBound = bytes.Clone(newBoundary)
		rewriteCurrentManifest(t, fixture.store, manifest)

		err := fixture.store.VerifyContext(context.Background())
		var corrupt *ErrCorrupt
		require.ErrorAs(t, err, &corrupt)
	})
}

func TestVerifyContextMissingPartRecoversAfterRestore(t *testing.T) {
	t.Parallel()

	fixture := newMultipartTierFixture(t, 1)
	readArchive := fixture.newReadArchive(
		t,
		coldstorage.NewFilesystemStorage(fixture.objectDir),
		"verify-restore-cache",
	)
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: readArchive}))
	_, part := findPart(t, fixture.manifest.Runs[0], func(part ArchivePart) bool {
		return partMatchesAny(part, effectiveAssetDataRanges(t, fixture.manifest.Runs[0].ID))
	})
	path := coldObjectPath(fixture.objectDir, part.Ref)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))

	err = fixture.store.VerifyContext(context.Background())
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	require.NoError(t, os.WriteFile(path, content, 0o644))
	require.NoError(t, fixture.store.VerifyContext(context.Background()))
}

func TestColdTieredCompactionKeepsRunCountLogarithmic(t *testing.T) {
	const (
		publications = 259
		threshold    = 4
	)

	root := t.TempDir()
	coldStore := openTestStoreAt(t, filepath.Join(root, "cold-history"))
	t.Cleanup(func() { require.NoError(t, coldStore.Close()) })
	reference := newTestStore(t)
	archive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID:  tierTestBucket,
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(root, "cold-cache"),
			CacheMaxBytes: 128 << 20,
		},
		noop.NewMeterProvider().Meter("balance-history-long-tier-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })
	require.NoError(t, coldStore.ConfigureTiering(TieringConfig{
		Archive:         archive,
		MinimumLevel:    2,
		RetainLocalRuns: 1,
		MaxSegmentBytes: 1 << 20,
		MaxRunsPerPass:  64,
	}))

	for sequence := uint64(1); sequence <= publications; sequence++ {
		publishBalanced(t, coldStore, sequence, sequence, sequence, sequence, sequence)
		_, err := coldStore.CompactContext(context.Background(), threshold)
		require.NoError(t, err)
		_, err = coldStore.Tier(context.Background())
		require.NoError(t, err)
	}
	for {
		coldChanged, err := coldStore.CompactContext(context.Background(), threshold)
		require.NoError(t, err)
		_, err = coldStore.Tier(context.Background())
		require.NoError(t, err)
		if !coldChanged {
			break
		}
	}
	referenceEffects := make([]balancehistory.Effect, 0, publications*2)
	for sequence := uint64(1); sequence <= publications; sequence++ {
		referenceEffects = append(referenceEffects,
			inputEffect(sequence, sequence, sequence, sequence, 7, "assets:cash", sequence),
			outputEffect(sequence, sequence, sequence, sequence, 7, "world", sequence),
		)
	}
	_, err = reference.Publish(Publication{
		Effects: referenceEffects,
		Coverage: Coverage{
			AuditSequence: publications, LogSequence: publications, SourceComplete: true,
		},
	})
	require.NoError(t, err)

	manifest, err := coldStore.Manifest()
	require.NoError(t, err)
	byLevel := make(map[uint32]int)
	coldRuns := 0
	for _, run := range manifest.Runs {
		byLevel[run.Level]++
		if run.LocalRemoved {
			coldRuns++
		}
	}
	for level, count := range byLevel {
		require.Less(t, count, threshold, "level %d retained too many runs", level)
	}
	levels := 1
	capacity := threshold
	for capacity < publications {
		capacity *= threshold
		levels++
	}
	runBound := (threshold - 1) * levels
	require.LessOrEqual(t, len(manifest.Runs), runBound)
	require.Positive(t, coldRuns)

	coldView, err := coldStore.OpenView(publications)
	require.NoError(t, err)
	referenceView, err := reference.OpenView(publications)
	require.NoError(t, err)
	coldDigest, err := coldView.SemanticDigest(context.Background())
	require.NoError(t, err)
	referenceDigest, err := referenceView.SemanticDigest(context.Background())
	require.NoError(t, err)
	require.Equal(t, referenceDigest, coldDigest)
	volumes, err := coldView.ReadVolumes(7, AxisEffective, publications, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.Equal(t, "33670", volumes[0].Input.String())
	require.NoError(t, coldView.Close())
	require.NoError(t, referenceView.Close())
}

func TestPreparedCompactionRunIsProtectedFromGarbageCollection(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence, sequence, sequence)
	}
	view, err := store.openCompactionView(context.Background())
	require.NoError(t, err)
	selected := selectCompactionRuns(view.manifest.Runs, 4)
	require.Len(t, selected, 4)
	runID, reserved, err := store.reserveCompactionRun(selected, view.generation)
	require.NoError(t, err)
	require.True(t, reserved)
	_, err = store.streamCompactedRun(context.Background(), view, selected, runID)
	require.NoError(t, err)

	_, err = store.CollectGarbage()
	require.NoError(t, err)
	snapshot := store.db.NewSnapshot()
	_, entries, identities, err := verifyRunRecordsContext(context.Background(), snapshot, runID)
	require.NoError(t, err)
	require.Positive(t, entries)
	require.Positive(t, identities)
	require.NoError(t, snapshot.Close())
	require.NoError(t, view.Close())
	require.NoError(t, store.discardPreparedRun(runID))
	_, closer, err := store.db.Get(runMetaKey(runID))
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestPreparedCompactionCrashLeavesRecoverableOrphan(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence, sequence, sequence)
	}
	view, err := store.openCompactionView(context.Background())
	require.NoError(t, err)
	selected := selectCompactionRuns(view.manifest.Runs, 4)
	runID, reserved, err := store.reserveCompactionRun(selected, view.generation)
	require.NoError(t, err)
	require.True(t, reserved)
	_, err = store.streamCompactedRun(context.Background(), view, selected, runID)
	require.NoError(t, err)
	require.NoError(t, view.Close())
	require.NoError(t, store.SyncWAL())
	require.NoError(t, store.Close())

	store = openTestStoreAt(t, dir)
	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Len(t, manifest.Runs, 4)
	collected, err := store.CollectGarbage()
	require.NoError(t, err)
	require.True(t, collected)
	compacted, err := store.CompactContext(context.Background(), 4)
	require.NoError(t, err)
	require.True(t, compacted)
	require.NoError(t, store.SyncWAL())
	require.NoError(t, store.Close())

	store = openTestStoreAt(t, dir)
	require.NoError(t, store.VerifyContext(context.Background()))
	manifest, err = store.Manifest()
	require.NoError(t, err)
	require.Len(t, manifest.Runs, 1)
	require.NoError(t, store.Close())
}

func TestTierRevalidatesDelayedArchiveBeforeDeletingLocalRun(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	publishBalanced(t, store, 1, 1, 1, 1, 1)
	pinned, err := store.OpenView(1)
	require.NoError(t, err)
	archive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID: tierTestBucket, OwnerID: "node-1", CacheDir: filepath.Join(root, "cache"), CacheMaxBytes: 64 << 20,
		},
		noop.NewMeterProvider().Meter("balance-history-tier-revalidation-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archive, MaxSegmentBytes: 4 << 10}))
	_, err = store.Tier(context.Background())
	require.NoError(t, err)
	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.True(t, manifest.Runs[0].Archived)
	require.False(t, manifest.Runs[0].LocalRemoved)
	require.NoError(t, pinned.Close())
	require.NoError(t, os.Remove(coldObjectPath(filepath.Join(root, "objects"), manifest.Runs[0].ArchiveParts[0].Ref)))

	_, err = store.Tier(context.Background())
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	manifest, err = store.Manifest()
	require.NoError(t, err)
	require.False(t, manifest.Runs[0].LocalRemoved)
	local, err := store.OpenView(1)
	require.NoError(t, err)
	volumes, err := local.ReadVolumes(7, AxisEffective, 1, []string{"assets:cash"})
	require.NoError(t, err)
	require.Equal(t, "1", volumes[0].Input.String())
	require.NoError(t, local.Close())
}

func TestColdCompactionMissingPartReturnsSourceMissing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	archive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID: tierTestBucket, OwnerID: "node-1", CacheDir: filepath.Join(root, "cache"), CacheMaxBytes: 64 << 20,
		},
		noop.NewMeterProvider().Meter("balance-history-missing-compaction-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })
	require.NoError(t, store.ConfigureTiering(TieringConfig{
		Archive: archive, MinimumLevel: 1, MaxSegmentBytes: 4 << 10, MaxRunsPerPass: 16,
	}))
	for sequence := uint64(1); sequence <= 16; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence, sequence, sequence)
		_, err := store.CompactContext(context.Background(), 4)
		require.NoError(t, err)
		_, err = store.Tier(context.Background())
		require.NoError(t, err)
	}
	manifest, err := store.Manifest()
	require.NoError(t, err)
	selected := selectCompactionRuns(manifest.Runs, 4)
	require.Len(t, selected, 4)
	require.True(t, selected[0].LocalRemoved)
	require.NoError(t, os.Remove(coldObjectPath(filepath.Join(root, "objects"), selected[0].ArchiveParts[0].Ref)))
	readArchive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID: tierTestBucket, OwnerID: "node-1", CacheDir: filepath.Join(root, "read-cache"), CacheMaxBytes: 64 << 20,
		},
		noop.NewMeterProvider().Meter("balance-history-missing-compaction-read-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, readArchive.Close()) })
	require.NoError(t, store.ConfigureTiering(TieringConfig{
		Archive: readArchive, MinimumLevel: 1, MaxSegmentBytes: 4 << 10, MaxRunsPerPass: 16,
	}))

	_, err = store.CompactContext(context.Background(), 4)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
}

func loadedArchiveParts(view *View) map[[32]byte]struct{} {
	loaded := make(map[[32]byte]struct{})
	for _, cold := range view.coldRuns {
		cold.mu.Lock()
		for _, part := range cold.parts {
			if part.reader != nil {
				loaded[part.meta.Ref.SHA256] = struct{}{}
			}
		}
		cold.mu.Unlock()
	}

	return loaded
}

type keyRange struct {
	lower []byte
	upper []byte
}

func effectiveAssetRanges(t *testing.T, runID uint64) []keyRange {
	t.Helper()

	catalog, err := catalogPrefix(runID, AxisEffective, scopeAsset, 7, nil)
	require.NoError(t, err)
	data, err := dataIdentityPrefix(runID, recordIdentity{
		Axis: AxisEffective, Scope: scopeAsset, LedgerID: 7, AssetBase: "USD", AssetPrecision: 2,
	})
	require.NoError(t, err)

	return []keyRange{{lower: catalog, upper: prefixEnd(catalog)}, {lower: data, upper: prefixEnd(data)}}
}

func effectiveAssetDataRanges(t *testing.T, runID uint64) []keyRange {
	t.Helper()

	data, err := dataIdentityPrefix(runID, recordIdentity{
		Axis: AxisEffective, Scope: scopeAsset, LedgerID: 7, AssetBase: "USD", AssetPrecision: 2,
	})
	require.NoError(t, err)

	return []keyRange{{lower: data, upper: prefixEnd(data)}}
}

func effectiveExactAccountRanges(t *testing.T, runID uint64, account string) []keyRange {
	t.Helper()

	catalog, err := catalogPrefix(runID, AxisEffective, scopeVolume, 7, &account)
	require.NoError(t, err)
	data, err := dataIdentityPrefix(runID, recordIdentity{
		Axis: AxisEffective, Scope: scopeVolume, LedgerID: 7, Account: account, AssetBase: "USD", AssetPrecision: 2,
	})
	require.NoError(t, err)

	return []keyRange{{lower: catalog, upper: prefixEnd(catalog)}, {lower: data, upper: prefixEnd(data)}}
}

func routeRanges(runID uint64, axis Axis, scope recordScope, ledgerID uint32) []keyRange {
	ranges := make([]keyRange, 0, 2)
	for _, kind := range []byte{prefixRunData, prefixRunCatalog} {
		prefix := append(runPrefix(kind, runID), byte(axis), byte(scope))
		prefix = binary.BigEndian.AppendUint32(prefix, ledgerID)
		ranges = append(ranges, keyRange{lower: prefix, upper: prefixEnd(prefix)})
	}

	return ranges
}

func partMatchesAny(part ArchivePart, ranges []keyRange) bool {
	for _, candidate := range ranges {
		if partIntersects(part, candidate.lower, candidate.upper) {
			return true
		}
	}

	return false
}

func findPart(t *testing.T, run RunRef, predicate func(ArchivePart) bool) (int, ArchivePart) {
	t.Helper()

	for index, part := range run.ArchiveParts {
		if predicate(part) {
			return index, part
		}
	}
	t.Fatalf("run %d has no matching archive part", run.ID)

	return 0, ArchivePart{}
}

func coldObjectPath(root string, ref balancehistoryarchive.Ref) string {
	bucketID, err := (balancehistoryarchive.Config{
		BaseBucketID: tierTestBucket,
		OwnerID:      "node-1",
	}).ObjectBucketID(ref)
	if err != nil {
		panic(err)
	}

	return filepath.Join(
		root,
		filepath.FromSlash(bucketID),
		"chapters",
		"0",
		"archive.sst",
	)
}

func tamperFile(t *testing.T, path string) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var value [1]byte
	_, err = file.ReadAt(value[:], 0)
	require.NoError(t, err)
	value[0] ^= 0xff
	_, err = file.WriteAt(value[:], 0)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
}

func rewriteCurrentManifest(t *testing.T, store *Store, manifest Manifest) {
	t.Helper()

	encoded, err := encodeManifest(manifest)
	require.NoError(t, err)
	require.NoError(t, store.db.Set(manifestKey(manifest.Version), encoded, pebble.Sync))
}

func readArchiveRecords(t *testing.T, archive balancehistoryarchive.Archive, ref balancehistoryarchive.Ref) []balancehistoryarchive.Record {
	t.Helper()

	lease, err := archive.Fetch(context.Background(), ref)
	require.NoError(t, err)
	reader, err := lease.Open()
	require.NoError(t, err)
	records := make([]balancehistoryarchive.Record, 0, ref.RecordCount)
	for reader.Next() {
		record := reader.Record()
		records = append(records, balancehistoryarchive.Record{
			Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value),
		})
	}
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())
	require.NoError(t, lease.Close())

	return records
}
