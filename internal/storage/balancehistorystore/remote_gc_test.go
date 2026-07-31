package balancehistorystore

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

type remoteGCTestFixture struct {
	store   *Store
	archive *balancehistoryarchive.Store
	now     time.Time
}

func newRemoteGCTestFixture(t *testing.T, owner string) *remoteGCTestFixture {
	t.Helper()

	root := t.TempDir()
	fixture := &remoteGCTestFixture{
		store: openTestStoreAt(t, filepath.Join(root, "history")),
		now:   time.Date(2026, time.July, 31, 10, 0, 0, 0, time.UTC),
	}
	var err error
	fixture.archive, err = balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID:  "remote-gc-test",
			OwnerID:       owner,
			CacheDir:      filepath.Join(root, "cache"),
			CacheMaxBytes: 16 << 20,
		},
		noop.NewMeterProvider().Meter("remote-gc-test"),
	)
	require.NoError(t, err)
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: fixture.archive}))
	t.Cleanup(func() {
		if fixture.store != nil {
			require.NoError(t, fixture.store.Close())
		}
		require.NoError(t, fixture.archive.Close())
	})

	return fixture
}

func (f *remoteGCTestFixture) collector(t *testing.T, grace time.Duration) *RemoteCollector {
	t.Helper()

	collector, err := NewRemoteCollector(f.store, RemoteCollectorConfig{
		GracePeriod: grace,
		Now:         func() time.Time { return f.now },
	})
	require.NoError(t, err)

	return collector
}

func archiveRemoteGCOrphan(t *testing.T, archive balancehistoryarchive.Archive, suffix string) balancehistoryarchive.Ref {
	t.Helper()

	ref, err := archive.Archive(context.Background(), balancehistoryarchive.NewSliceStream([]balancehistoryarchive.Record{
		{Key: []byte("orphan/" + suffix), Value: []byte("value/" + suffix)},
	}))
	require.NoError(t, err)

	return ref
}

func requireRemoteGCObjectExists(t *testing.T, archive balancehistoryarchive.Archive, ref balancehistoryarchive.Ref, want bool) {
	t.Helper()

	exists, err := archive.Exists(context.Background(), ref)
	require.NoError(t, err)
	require.Equal(t, want, exists)
}

func TestRemoteCollectorRequiresReclaimableConfiguredArchive(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	archive := NewMockIdentifiedArchive(gomock.NewController(t))
	archive.EXPECT().DestinationIdentity().AnyTimes().Return("test-non-reclaimable-archive-v1")
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archive}))

	_, err := NewRemoteCollector(store, RemoteCollectorConfig{})
	require.ErrorIs(t, err, balancehistoryarchive.ErrReclamationUnsupported)
}

func TestRemoteCollectorRequiresGraceAndASecondObservation(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-grace")
	ref := archiveRemoteGCOrphan(t, fixture.archive, "grace")
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}

	first, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.QueueObjects)
	require.Zero(t, first.DeletedObjects)
	requireRemoteGCObjectExists(t, fixture.archive, ref, true)

	fixture.now = fixture.now.Add(30 * time.Minute)
	second, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(1), second.QueueObjects)
	require.Zero(t, second.DeletedObjects)
	requireRemoteGCObjectExists(t, fixture.archive, ref, true)

	fixture.now = fixture.now.Add(31 * time.Minute)
	third, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(1), third.DeletedObjects)
	require.Equal(t, ref.Size, third.DeletedBytes)
	require.Zero(t, third.QueueObjects)
	requireRemoteGCObjectExists(t, fixture.archive, ref, false)
}

func TestRemoteCollectorRetriesDeleteAfterCrashBeforeAck(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-crash")
	ref := archiveRemoteGCOrphan(t, fixture.archive, "crash")
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}

	_, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	fixture.now = fixture.now.Add(2 * time.Hour)
	crashed := errors.New("simulated crash after delete")
	collector.hooks.afterDeleteBeforeAck = func(digest [32]byte) error {
		require.Equal(t, ref.SHA256, digest)

		return crashed
	}
	result, err := collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, crashed)
	require.Zero(t, result.DeletedObjects)
	require.Equal(t, uint64(1), result.QueueObjects)
	requireRemoteGCObjectExists(t, fixture.archive, ref, false)

	collector.hooks.afterDeleteBeforeAck = nil
	retried, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(1), retried.DeletedObjects)
	require.Zero(t, retried.QueueObjects)
}

func TestRemoteCollectorResumesDurableCursorAndCandidatesAfterRestart(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	historyDir := filepath.Join(root, "history")
	archive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID:  "remote-gc-restart",
			OwnerID:       "node-restart",
			CacheDir:      filepath.Join(root, "cache"),
			CacheMaxBytes: 16 << 20,
		},
		noop.NewMeterProvider().Meter("remote-gc-restart"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })
	for index := range 3 {
		archiveRemoteGCOrphan(t, archive, string(rune('a'+index)))
	}

	now := time.Date(2026, time.July, 31, 11, 0, 0, 0, time.UTC)
	store := openTestStoreAt(t, historyDir)
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archive}))
	collector, err := NewRemoteCollector(store, RemoteCollectorConfig{
		GracePeriod: time.Hour,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	first, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 1, DeleteLimit: 10})
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.QueueObjects)
	require.False(t, first.CycleCompleted)
	require.NoError(t, store.Close())

	store = openTestStoreAt(t, historyDir)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archive}))
	collector, err = NewRemoteCollector(store, RemoteCollectorConfig{
		GracePeriod: time.Hour,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	second, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 1, DeleteLimit: 10})
	require.NoError(t, err)
	require.Equal(t, uint64(2), second.QueueObjects)
	require.False(t, second.CycleCompleted)
	third, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 1, DeleteLimit: 10})
	require.NoError(t, err)
	require.Equal(t, uint64(3), third.QueueObjects)
	require.True(t, third.CycleCompleted)
}

func TestRemoteCollectorBlocksAllDeletionAcrossResetManifestABA(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-reset")
	manifest := publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	require.Equal(t, uint64(1), manifest.Version)
	view, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	ref := archiveRemoteGCOrphan(t, fixture.archive, "reset")
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}

	_, err = collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	fixture.now = fixture.now.Add(2 * time.Hour)
	require.NoError(t, fixture.store.Reset())
	rebuilt := publishBalanced(t, fixture.store, 1, 1, 2, 2, 2)
	require.Equal(t, manifest.Version, rebuilt.Version, "test must exercise version ABA")

	blocked, err := collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	require.Zero(t, blocked.DeletedObjects)
	require.Equal(t, uint64(1), blocked.QueueObjects)
	requireRemoteGCObjectExists(t, fixture.archive, ref, true)

	require.NoError(t, view.Close())
	collected, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(1), collected.DeletedObjects)
	requireRemoteGCObjectExists(t, fixture.archive, ref, false)
}

func TestRemoteCollectorRetiresLatestManifestRootsWithoutDeletion(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-root")
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	changed, err := fixture.store.Tier(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	manifest, err := fixture.store.Manifest()
	require.NoError(t, err)
	require.Len(t, manifest.Runs, 1)
	require.NotEmpty(t, manifest.Runs[0].ArchiveParts)
	refs := make([]balancehistoryarchive.Ref, 0, len(manifest.Runs[0].ArchiveParts))
	for _, part := range manifest.Runs[0].ArchiveParts {
		refs = append(refs, part.Ref)
	}
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}

	_, err = collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	fixture.now = fixture.now.Add(2 * time.Hour)
	rooted, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(len(refs)), rooted.RetiredRooted)
	require.Zero(t, rooted.DeletedObjects)
	require.Zero(t, rooted.QueueObjects)
	for _, ref := range refs {
		requireRemoteGCObjectExists(t, fixture.archive, ref, true)
	}
}

func TestTierSkipsCandidateWhenArchiveIsDisabledBeforeGate(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-disable")
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	_, err := fixture.collector(t, time.Hour).Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	state := fixture.store.tiering.Load()
	reached := make(chan struct{})
	resume := make(chan struct{})
	state.beforeGate = func() {
		close(reached)
		<-resume
	}

	type tierResult struct {
		changed int
		err     error
	}
	done := make(chan tierResult, 1)
	go func() {
		changed, err := fixture.store.Tier(context.Background())
		done <- tierResult{changed: changed, err: err}
	}()
	<-reached
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{}))
	close(resume)
	result := <-done
	require.NoError(t, result.err)
	require.Zero(t, result.changed)
	page, err := fixture.archive.List(context.Background(), "", 10)
	require.NoError(t, err)
	require.Empty(t, page.Objects)
}

func TestTierSkipsCandidateWhenArchiveNamespaceChangesBeforeGate(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-old")
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	_, err := fixture.collector(t, time.Hour).Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	state := fixture.store.tiering.Load()
	reached := make(chan struct{})
	resume := make(chan struct{})
	state.beforeGate = func() {
		close(reached)
		<-resume
	}

	root := t.TempDir()
	replacement, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "objects")),
		balancehistoryarchive.Config{
			BaseBucketID:  "remote-gc-test",
			OwnerID:       "node-new",
			CacheDir:      filepath.Join(root, "cache"),
			CacheMaxBytes: 16 << 20,
		},
		noop.NewMeterProvider().Meter("remote-gc-replacement"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replacement.Close()) })

	type tierResult struct {
		changed int
		err     error
	}
	done := make(chan tierResult, 1)
	go func() {
		changed, err := fixture.store.Tier(context.Background())
		done <- tierResult{changed: changed, err: err}
	}()
	<-reached
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{Archive: replacement}))
	close(resume)
	result := <-done
	require.NoError(t, result.err)
	require.Zero(t, result.changed)
	oldPage, err := fixture.archive.List(context.Background(), "", 10)
	require.NoError(t, err)
	require.Empty(t, oldPage.Objects)
	newPage, err := replacement.List(context.Background(), "", 10)
	require.NoError(t, err)
	require.Empty(t, newPage.Objects)
}

func TestRemoteCollectorWriterGateWaitsForInFlightArchive(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-gate")
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	view, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	ref := archiveRemoteGCOrphan(t, fixture.archive, "gate")
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}
	_, err = collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	fixture.now = fixture.now.Add(2 * time.Hour)
	_, err = collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	require.NoError(t, view.Close())

	tiering := fixture.store.tiering.Load()
	reached := make(chan struct{})
	resume := make(chan struct{})
	collector.hooks.beforeArchiveGate = func() {
		close(reached)
		<-resume
	}
	type collectResult struct {
		result RemoteGCResult
		err    error
	}
	done := make(chan collectResult, 1)
	go func() {
		result, err := collector.Collect(context.Background(), budget)
		done <- collectResult{result: result, err: err}
	}()
	<-reached
	tiering.archiveGate.RLock()
	close(resume)
	requireRemoteGCObjectExists(t, fixture.archive, ref, true)
	select {
	case <-done:
		require.Fail(t, "collector crossed a held archive read gate")
	default:
	}
	tiering.archiveGate.RUnlock()
	collected := <-done
	require.NoError(t, collected.err)
	require.Equal(t, uint64(1), collected.result.DeletedObjects)
	requireRemoteGCObjectExists(t, fixture.archive, ref, false)
}

func TestRemoteCollectorSerializesConcurrentCalls(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-serialize")
	archiveRemoteGCOrphan(t, fixture.archive, "serialize")
	collector := fixture.collector(t, time.Hour)
	reached := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	collector.hooks.afterListBeforeSync = func() error {
		once.Do(func() {
			close(reached)
			<-resume
		})

		return nil
	}
	done := make(chan error, 2)
	go func() {
		_, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
		done <- err
	}()
	<-reached
	go func() {
		_, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
		done <- err
	}()
	select {
	case <-done:
		require.Fail(t, "concurrent collector call bypassed the collector mutex")
	default:
	}
	close(resume)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
}

func TestRemoteCollectorPrunesFirstObservationThatVanishesAcrossPagedScans(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-prune")
	refs := make([]balancehistoryarchive.Ref, 0, 3)
	for index := range 3 {
		refs = append(refs, archiveRemoteGCOrphan(t, fixture.archive, string(rune('a'+index))))
	}
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 1, DeleteLimit: 10}

	first, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.QueueObjects)
	require.False(t, first.CycleCompleted)
	page, err := fixture.archive.List(context.Background(), "", 1)
	require.NoError(t, err)
	require.Len(t, page.Objects, 1)
	require.NoError(t, fixture.archive.Delete(context.Background(), page.Objects[0].SHA256))

	second, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.False(t, second.CycleCompleted)
	third, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.True(t, third.CycleCompleted)
	require.Equal(t, uint64(3), third.QueueObjects)

	fourth, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.False(t, fourth.CycleCompleted)
	fifth, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.True(t, fifth.CycleCompleted)
	require.Equal(t, uint64(2), fifth.QueueObjects)
	for _, ref := range refs[1:] {
		requireRemoteGCObjectExists(t, fixture.archive, ref, true)
	}
}

func TestRemoteCollectorsSerializeInventoryDuringDeleteAck(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-multi-collector")
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	view, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	archiveRemoteGCOrphan(t, fixture.archive, "multi-collector")
	first := fixture.collector(t, time.Hour)
	second := fixture.collector(t, time.Hour)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	registration, err := second.RegisterMetrics(provider.Meter("remote-gc-concurrent-result-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, registration.Unregister()) })
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}
	_, err = first.Collect(context.Background(), budget)
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	fixture.now = fixture.now.Add(2 * time.Hour)
	_, err = first.Collect(context.Background(), budget)
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	require.NoError(t, view.Close())

	deleteReached := make(chan struct{})
	resumeDeleteAck := make(chan struct{})
	first.hooks.afterDeleteBeforeAck = func([32]byte) error {
		close(deleteReached)
		<-resumeDeleteAck

		return nil
	}
	type collectResult struct {
		result RemoteGCResult
		err    error
	}
	firstDone := make(chan collectResult, 1)
	go func() {
		result, err := first.Collect(context.Background(), budget)
		firstDone <- collectResult{result: result, err: err}
	}()
	<-deleteReached

	secondListed := make(chan struct{})
	second.hooks.afterListBeforeSync = func() error {
		close(secondListed)

		return nil
	}
	secondDone := make(chan collectResult, 1)
	go func() {
		result, err := second.Collect(context.Background(), budget)
		secondDone <- collectResult{result: result, err: err}
	}()
	select {
	case <-secondListed:
		require.Fail(t, "concurrent inventory crossed an in-flight delete acknowledgement")
	default:
	}
	stateBeforeAck, found, err := readRemoteGCState(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	close(resumeDeleteAck)
	firstResult := <-firstDone
	require.NoError(t, firstResult.err)
	require.Equal(t, uint64(1), firstResult.result.DeletedObjects)
	<-secondListed
	secondResult := <-secondDone
	require.NoError(t, secondResult.err)
	require.Zero(t, secondResult.result.QueueObjects)

	stateAfterAck, found, err := readRemoteGCState(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Greater(t, stateAfterAck.Cycle, stateBeforeAck.Cycle)
	require.Zero(t, stateAfterAck.QueueObjects)
	metrics := readRemoteGCMetrics(t, reader)
	require.Zero(t, metrics.integers["balancehistory.remote_gc.queue.objects"])
}

func TestRemoteCollectorsRefreshPostSyncStateWhenActiveViewBlocksBoth(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-multi-collector-view")
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	view, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	archiveRemoteGCOrphan(t, fixture.archive, "multi-view-a")
	archiveRemoteGCOrphan(t, fixture.archive, "multi-view-b")

	first := fixture.collector(t, time.Hour)
	second := fixture.collector(t, time.Hour)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	registration, err := first.RegisterMetrics(provider.Meter("remote-gc-multi-view-refresh-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, registration.Unregister()) })

	firstReachedGate := make(chan struct{})
	resumeFirst := make(chan struct{})
	first.hooks.beforeArchiveGate = func() {
		close(firstReachedGate)
		<-resumeFirst
	}
	type collectResult struct {
		result RemoteGCResult
		err    error
	}
	firstDone := make(chan collectResult, 1)
	go func() {
		result, err := first.Collect(context.Background(), RemoteGCBudget{ScanLimit: 1, DeleteLimit: 10})
		firstDone <- collectResult{result: result, err: err}
	}()
	<-firstReachedGate

	secondResult, err := second.Collect(context.Background(), RemoteGCBudget{ScanLimit: 1, DeleteLimit: 10})
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	require.Equal(t, uint64(2), secondResult.QueueObjects)
	close(resumeFirst)
	firstResult := <-firstDone
	require.ErrorIs(t, firstResult.err, ErrRemoteGCRootsUnavailable)
	require.Equal(t, uint64(2), firstResult.result.QueueObjects)
	require.Equal(t, int64(2), readRemoteGCMetrics(t, reader).integers["balancehistory.remote_gc.queue.objects"])
}

func TestRemoteCollectorRefreshIsLinearizedBeforeArchiveGateRelease(t *testing.T) {
	t.Parallel()

	type collectResult struct {
		result RemoteGCResult
		err    error
	}
	newRotationFixture := func(t *testing.T, suffix string) (*Store, *balancehistoryarchive.Store, *balancehistoryarchive.Store) {
		t.Helper()

		root := t.TempDir()
		store := openTestStoreAt(t, filepath.Join(root, "history"))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-refresh-"+suffix)
		archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-refresh-"+suffix)
		require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
		archiveRemoteGCOrphan(t, archiveB, "new-destination-"+suffix)

		return store, archiveA, archiveB
	}
	registerMetrics := func(t *testing.T, collector *RemoteCollector, name string) *sdkmetric.ManualReader {
		t.Helper()

		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
		registration, err := collector.RegisterMetrics(provider.Meter(name))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, registration.Unregister()) })

		return reader
	}

	t.Run("successful result is stable across later rotation and collection", func(t *testing.T) {
		store, _, archiveB := newRotationFixture(t, "success")
		first, err := NewRemoteCollector(store, RemoteCollectorConfig{})
		require.NoError(t, err)
		firstReader := registerMetrics(t, first, "remote-gc-refresh-before-unlock-success-first")
		phaseCompleted := make(chan struct{})
		resume := make(chan struct{})
		first.hooks.afterArchiveGatePhase = func() {
			close(phaseCompleted)
			<-resume
		}
		firstDone := make(chan collectResult, 1)
		go func() {
			result, err := first.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
			firstDone <- collectResult{result: result, err: err}
		}()
		<-phaseCompleted

		require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveB}))
		second, err := NewRemoteCollector(store, RemoteCollectorConfig{})
		require.NoError(t, err)
		secondReader := registerMetrics(t, second, "remote-gc-refresh-before-unlock-success-second")
		secondResult, err := second.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
		require.NoError(t, err)
		require.Equal(t, uint64(1), secondResult.QueueObjects)

		close(resume)
		firstResult := <-firstDone
		require.NoError(t, firstResult.err)
		require.Zero(t, firstResult.result.QueueObjects)
		require.Zero(t, readRemoteGCMetrics(t, firstReader).integers["balancehistory.remote_gc.queue.objects"])
		require.Equal(t, int64(1), readRemoteGCMetrics(t, secondReader).integers["balancehistory.remote_gc.queue.objects"])
	})

	t.Run("reconfiguration error wins over refresh mismatch", func(t *testing.T) {
		store, _, archiveB := newRotationFixture(t, "reconfigured")
		first, err := NewRemoteCollector(store, RemoteCollectorConfig{})
		require.NoError(t, err)
		inventoryCompleted := make(chan struct{})
		resume := make(chan struct{})
		first.hooks.afterInventoryPage = func() {
			close(inventoryCompleted)
			<-resume
		}
		firstDone := make(chan collectResult, 1)
		go func() {
			result, err := first.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
			firstDone <- collectResult{result: result, err: err}
		}()
		<-inventoryCompleted

		require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveB}))
		second, err := NewRemoteCollector(store, RemoteCollectorConfig{})
		require.NoError(t, err)
		secondResult, err := second.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
		require.NoError(t, err)
		require.Equal(t, uint64(1), secondResult.QueueObjects)

		close(resume)
		firstResult := <-firstDone
		require.Equal(t, errTieringReconfigured, firstResult.err)
	})
}

func TestRemoteCollectorRestartsScanWhenResetMutatesArchiveEpochAfterList(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-scan-epoch")
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}
	_, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	proof, found, err := readRemoteGCState(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, proof.ScanEpoch, proof.CompletedInventoryEpoch)

	listed := make(chan struct{})
	resume := make(chan struct{})
	collector.hooks.afterListBeforeSync = func() error {
		close(listed)
		<-resume

		return nil
	}
	type collectResult struct {
		result RemoteGCResult
		err    error
	}
	done := make(chan collectResult, 1)
	go func() {
		result, err := collector.Collect(context.Background(), budget)
		done <- collectResult{result: result, err: err}
	}()
	<-listed
	require.NoError(t, fixture.store.Reset())
	close(resume)
	collected := <-done
	require.NoError(t, collected.err)
	require.False(t, collected.result.CycleCompleted)
	require.Zero(t, collected.result.ScannedObjects)

	binding, found, err := readArchiveBindingRecord(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	state, found, err := readRemoteGCState(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, binding.MutationEpoch, state.ScanEpoch)
	require.NotEqual(t, binding.MutationEpoch, state.CompletedInventoryEpoch)
	require.Empty(t, state.Cursor)
	require.Zero(t, state.ScanObjects)
	require.Zero(t, state.ScanBytes)

	collector.hooks.afterListBeforeSync = nil
	fresh, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.True(t, fresh.CycleCompleted)
	state, found, err = readRemoteGCState(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, binding.MutationEpoch, state.CompletedInventoryEpoch)
}

func TestRemoteCollectorSerializesInventoryPageWithArchiveUpload(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-inventory-upload-gate")
	collector := fixture.collector(t, time.Hour)
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)

	listed := make(chan struct{})
	resumeInventory := make(chan struct{})
	collector.hooks.afterListBeforeSync = func() error {
		close(listed)
		<-resumeInventory

		return nil
	}
	collectDone := make(chan error, 1)
	go func() {
		_, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
		collectDone <- err
	}()
	<-listed

	type tierResult struct {
		changed int
		err     error
	}
	tierDone := make(chan tierResult, 1)
	go func() {
		changed, err := fixture.store.Tier(context.Background())
		tierDone <- tierResult{changed: changed, err: err}
	}()
	select {
	case <-tierDone:
		require.Fail(t, "tier upload crossed an in-flight inventory page")
	default:
	}
	close(resumeInventory)
	require.NoError(t, <-collectDone)
	tiered := <-tierDone
	require.NoError(t, tiered.err)
	require.Equal(t, 1, tiered.changed)

	binding, found, err := readArchiveBindingRecord(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	state, found, err := readRemoteGCState(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, binding.MutationEpoch, state.CompletedInventoryEpoch)
}

func TestRemoteCollectorWaitsForCASLossUploadThenInventoriesOrphan(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-inventory-cas-loss")
	require.NoError(t, fixture.store.ConfigureTiering(TieringConfig{
		Archive:        fixture.archive,
		MaxRunsPerPass: 1,
	}))
	collector := fixture.collector(t, time.Hour)
	budget := RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100}
	_, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		publishBalanced(t, fixture.store, sequence, sequence, sequence, sequence, sequence)
	}

	mutationAdvanced := make(chan struct{})
	resumeArchive := make(chan struct{})
	uploaded := make(chan struct{})
	resumePublish := make(chan struct{})
	tiering := fixture.store.tiering.Load()
	tiering.afterMutationBeforeArchive = func() {
		close(mutationAdvanced)
		<-resumeArchive
	}
	tiering.afterArchiveBeforePublish = func() {
		close(uploaded)
		<-resumePublish
	}
	type tierResult struct {
		changed int
		err     error
	}
	tierDone := make(chan tierResult, 1)
	go func() {
		changed, err := fixture.store.Tier(context.Background())
		tierDone <- tierResult{changed: changed, err: err}
	}()
	<-mutationAdvanced

	listed := make(chan struct{})
	inventoryGateAttempted := make(chan struct{})
	collector.hooks.beforeInventoryGate = func() { close(inventoryGateAttempted) }
	collector.hooks.afterListBeforeSync = func() error {
		close(listed)

		return nil
	}
	type collectResult struct {
		result RemoteGCResult
		err    error
	}
	collectDone := make(chan collectResult, 1)
	go func() {
		result, err := collector.Collect(context.Background(), budget)
		collectDone <- collectResult{result: result, err: err}
	}()
	<-inventoryGateAttempted
	select {
	case <-listed:
		require.Fail(t, "inventory listed while the pre-archive mutation window was open")
	case <-collectDone:
		require.Fail(t, "inventory completed while the pre-archive mutation window was open")
	default:
	}

	close(resumeArchive)
	<-uploaded
	compacted, err := fixture.store.CompactContext(context.Background(), 4)
	require.NoError(t, err)
	require.True(t, compacted)
	close(resumePublish)
	tiered := <-tierDone
	require.NoError(t, tiered.err)
	require.Zero(t, tiered.changed)

	collected := <-collectDone
	require.NoError(t, collected.err)
	require.True(t, collected.result.CycleCompleted)
	require.Positive(t, collected.result.ScannedObjects)
	require.Positive(t, collected.result.QueueObjects)
	page, err := fixture.archive.List(context.Background(), "", 100)
	require.NoError(t, err)
	require.NotEmpty(t, page.Objects)

	binding, found, err := readArchiveBindingRecord(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	state, found, err := readRemoteGCState(fixture.store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, binding.MutationEpoch, state.CompletedInventoryEpoch)
	require.Positive(t, state.InventoryObjects)
	require.Positive(t, state.QueueObjects)
}

func TestRemoteCollectorRefusesDestinationAbandonAndRotatesAfterDurableEmptyProof(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-same")
	archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-same")
	require.Equal(t, archiveA.Namespace(), archiveB.Namespace())
	require.NotEqual(t, archiveA.DestinationIdentity(), archiveB.DestinationIdentity())
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	ref := archiveRemoteGCOrphan(t, archiveA, "destination-a")
	now := time.Date(2026, time.July, 31, 12, 0, 0, 0, time.UTC)
	collectorA, err := NewRemoteCollector(store, RemoteCollectorConfig{
		GracePeriod: time.Hour,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	queued, err := collectorA.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	require.Equal(t, uint64(1), queued.QueueObjects)

	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "not been durably proven empty")
	requireRemoteGCObjectExists(t, archiveA, ref, true)

	now = now.Add(2 * time.Hour)
	deleted, err := collectorA.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	require.Equal(t, uint64(1), deleted.DeletedObjects)
	_, err = collectorA.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	require.NoError(t, store.Reset())
	binding, found, err := readArchiveBinding(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, archiveA.DestinationIdentity(), binding)
	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "not been durably proven empty")
	_, err = collectorA.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveB}))
	collectorB, err := NewRemoteCollector(store, RemoteCollectorConfig{
		GracePeriod: time.Hour,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	reset, err := collectorB.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	require.Zero(t, reset.QueueObjects)
	require.Zero(t, reset.DeletedObjects)
	requireRemoteGCObjectExists(t, archiveA, ref, false)
	state, found, err := readRemoteGCState(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, archiveB.DestinationIdentity(), state.DestinationIdentity)
}

func TestArchiveBindingAllowsEmptyRotationAndRejectsArchivedRotation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-binding")
	archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-binding")

	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	binding, found, err := readArchiveBinding(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, archiveA.DestinationIdentity(), binding)
	err = store.ConfigureTiering(TieringConfig{})
	require.ErrorContains(t, err, "not been durably proven empty")
	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "not been durably proven empty")
	collector, err := NewRemoteCollector(store, RemoteCollectorConfig{})
	require.NoError(t, err)
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveB}))
	binding, found, err = readArchiveBinding(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, archiveB.DestinationIdentity(), binding)

	publishBalanced(t, store, 1, 1, 1, 1, 1)
	changed, err := store.Tier(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	err = store.ConfigureTiering(TieringConfig{Archive: archiveA})
	require.ErrorContains(t, err, "destination mismatch")
	err = store.ConfigureTiering(TieringConfig{})
	require.ErrorContains(t, err, "cannot be disabled")
	require.Equal(t, archiveB.DestinationIdentity(), store.tiering.Load().archiveIdentity)
}

func TestArchiveBindingSurvivesResetAndRefusesQueuedRotation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-reset-binding")
	archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-reset-binding")
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	archiveRemoteGCOrphan(t, archiveA, "queued-before-reset")
	collector, err := NewRemoteCollector(store, RemoteCollectorConfig{})
	require.NoError(t, err)
	queued, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.NoError(t, err)
	require.Equal(t, uint64(1), queued.QueueObjects)
	require.NoError(t, store.Reset())

	binding, found, err := readArchiveBinding(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, archiveA.DestinationIdentity(), binding)
	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "not been durably proven empty")
	err = store.ConfigureTiering(TieringConfig{})
	require.ErrorContains(t, err, "not been durably proven empty")
	require.Equal(t, archiveA.DestinationIdentity(), store.tiering.Load().archiveIdentity)
}

func TestArchiveRotationRequiresCompletedInventoryAfterListFailureAndRestart(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	historyDir := filepath.Join(root, "history")
	archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-list-failure")
	archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-list-failure")
	store := openTestStoreAt(t, historyDir)
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	collector, err := NewRemoteCollector(store, RemoteCollectorConfig{})
	require.NoError(t, err)
	listFailure := errors.New("inventory list failed")
	mock := NewMockReclaimer(gomock.NewController(t))
	mock.EXPECT().Namespace().AnyTimes().Return(archiveA.Namespace())
	mock.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).Return(balancehistoryarchive.RemoteObjectPage{}, listFailure)
	store.tiering.Load().reclaimer = mock
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.ErrorIs(t, err, listFailure)
	state, found, err := readRemoteGCState(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, state.CompletedInventoryEpoch)
	require.NoError(t, store.Close())

	store = openTestStoreAt(t, historyDir)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "not been durably proven empty")
	err = store.ConfigureTiering(TieringConfig{})
	require.ErrorContains(t, err, "not been durably proven empty")
}

func TestArchiveResetPathsAdvanceMutationEpochAndInvalidateProof(t *testing.T) {
	t.Parallel()

	tests := map[string]func(t *testing.T, store *Store){
		"reset": func(t *testing.T, store *Store) {
			t.Helper()
			require.NoError(t, store.Reset())
		},
		"rebuild": func(t *testing.T, store *Store) {
			t.Helper()
			require.NoError(t, store.ResetForRebuild())
		},
		"source repair": func(t *testing.T, store *Store) {
			t.Helper()
			require.NoError(t, store.MarkSourceMissing("test source repair"))
			require.NoError(t, store.ResetForSourceRepair())
		},
	}
	for name, reset := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			store := openTestStoreAt(t, filepath.Join(root, "history"))
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-reset-epoch")
			archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-reset-epoch")
			require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
			collector, err := NewRemoteCollector(store, RemoteCollectorConfig{})
			require.NoError(t, err)
			_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
			require.NoError(t, err)
			before, found, err := readArchiveBindingRecord(store.db)
			require.NoError(t, err)
			require.True(t, found)
			state, found, err := readRemoteGCState(store.db)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, before.MutationEpoch, state.CompletedInventoryEpoch)

			reset(t, store)
			after, found, err := readArchiveBindingRecord(store.db)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, before.MutationEpoch+1, after.MutationEpoch)
			err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
			require.ErrorContains(t, err, "not been durably proven empty")
		})
	}
}

func TestArchiveUploadThenResetRequiresFreshEmptyInventoryBeforeRotation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-upload-reset")
	archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-upload-reset")
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	now := time.Date(2026, time.July, 31, 15, 0, 0, 0, time.UTC)
	collector, err := NewRemoteCollector(store, RemoteCollectorConfig{
		GracePeriod: time.Hour,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	initial, found, err := readArchiveBindingRecord(store.db)
	require.NoError(t, err)
	require.True(t, found)

	publishBalanced(t, store, 1, 1, 1, 1, 1)
	_, err = store.Tier(context.Background())
	require.NoError(t, err)
	afterUpload, found, err := readArchiveBindingRecord(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, initial.MutationEpoch+1, afterUpload.MutationEpoch)
	require.NoError(t, store.Reset())
	afterReset, found, err := readArchiveBindingRecord(store.db)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, afterUpload.MutationEpoch+1, afterReset.MutationEpoch)
	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "not been durably proven empty")

	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	now = now.Add(2 * time.Hour)
	deleted, err := collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	require.Positive(t, deleted.DeletedObjects)
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveB}))
}

func TestArchiveCASLossOrphanInvalidatesEmptyInventoryProof(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-cas-loss")
	archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-cas-loss")
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA, MaxRunsPerPass: 1}))
	now := time.Date(2026, time.July, 31, 16, 0, 0, 0, time.UTC)
	collector, err := NewRemoteCollector(store, RemoteCollectorConfig{
		GracePeriod: time.Hour,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence, sequence, sequence)
	}

	uploaded := make(chan struct{})
	resume := make(chan struct{})
	store.tiering.Load().afterArchiveBeforePublish = func() {
		close(uploaded)
		<-resume
	}
	type tierResult struct {
		changed int
		err     error
	}
	tierDone := make(chan tierResult, 1)
	go func() {
		changed, err := store.Tier(context.Background())
		tierDone <- tierResult{changed: changed, err: err}
	}()
	<-uploaded
	compacted, err := store.CompactContext(context.Background(), 4)
	require.NoError(t, err)
	require.True(t, compacted)
	close(resume)
	tiered := <-tierDone
	require.NoError(t, tiered.err)
	require.Zero(t, tiered.changed)
	page, err := archiveA.List(context.Background(), "", 100)
	require.NoError(t, err)
	require.NotEmpty(t, page.Objects)
	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "not been durably proven empty")

	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	now = now.Add(2 * time.Hour)
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 100, DeleteLimit: 100})
	require.NoError(t, err)
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveB}))
}

func TestArchiveBindingRejectsWrongDestinationAfterRestartAndMissingBinding(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	historyDir := filepath.Join(root, "history")
	archiveA := newRemoteGCTestArchive(t, filepath.Join(root, "objects-a"), filepath.Join(root, "cache-a"), "node-restart-binding")
	archiveB := newRemoteGCTestArchive(t, filepath.Join(root, "objects-b"), filepath.Join(root, "cache-b"), "node-restart-binding")
	store := openTestStoreAt(t, historyDir)
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	publishBalanced(t, store, 1, 1, 1, 1, 1)
	_, err := store.Tier(context.Background())
	require.NoError(t, err)
	require.NoError(t, store.SyncWAL())
	require.NoError(t, store.Close())

	store = openTestStoreAt(t, historyDir)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	err = store.ConfigureTiering(TieringConfig{Archive: archiveB})
	require.ErrorContains(t, err, "destination mismatch")
	require.NoError(t, store.ConfigureTiering(TieringConfig{Archive: archiveA}))
	view, err := store.OpenView(1)
	require.NoError(t, err)
	require.NoError(t, view.Close())

	require.NoError(t, store.db.Delete(archiveBindingKey(), pebble.Sync))
	err = store.ConfigureTiering(TieringConfig{Archive: archiveA})
	require.ErrorContains(t, err, "missing their destination binding")
	collector, err := NewRemoteCollector(store, RemoteCollectorConfig{})
	require.NoError(t, err)
	_, err = collector.Collect(context.Background(), RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10})
	require.ErrorContains(t, err, "archive binding")
}

func TestInitialConfigurePublishesNoPartialTieringState(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := openTestStoreAt(t, filepath.Join(root, "history"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	archive := newRemoteGCTestArchive(t, filepath.Join(root, "objects"), filepath.Join(root, "cache"), "node-initial")
	publishBalanced(t, store, 1, 1, 1, 1, 1)

	store.mutationMu.Lock()
	configureStarted := make(chan struct{})
	configureDone := make(chan error, 1)
	go func() {
		close(configureStarted)
		configureDone <- store.ConfigureTiering(TieringConfig{Archive: archive})
	}()
	<-configureStarted
	require.Nil(t, store.tiering.Load())
	changed, err := store.Tier(context.Background())
	require.NoError(t, err)
	require.Zero(t, changed)
	_, err = NewRemoteCollector(store, RemoteCollectorConfig{})
	require.ErrorIs(t, err, balancehistoryarchive.ErrReclamationUnsupported)

	viewStarted := make(chan struct{})
	viewDone := make(chan error, 1)
	go func() {
		close(viewStarted)
		view, err := store.OpenView(1)
		if err == nil {
			err = view.Close()
		}
		viewDone <- err
	}()
	<-viewStarted
	select {
	case <-configureDone:
		require.Fail(t, "ConfigureTiering bypassed the held mutation lock")
	case <-viewDone:
		require.Fail(t, "OpenView bypassed the held mutation lock")
	default:
	}
	store.mutationMu.Unlock()
	require.NoError(t, <-configureDone)
	require.NoError(t, <-viewDone)
	state := store.tiering.Load()
	require.NotNil(t, state)
	require.NotNil(t, state.archive)
	require.NotNil(t, state.archiveGate)
	require.NotEmpty(t, state.archiveIdentity)
}

func newRemoteGCTestArchive(
	t *testing.T,
	objectsDir string,
	cacheDir string,
	owner string,
) *balancehistoryarchive.Store {
	t.Helper()

	archive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(objectsDir),
		balancehistoryarchive.Config{
			BaseBucketID:  "remote-gc-same-namespace",
			OwnerID:       owner,
			CacheDir:      cacheDir,
			CacheMaxBytes: 16 << 20,
		},
		noop.NewMeterProvider().Meter("remote-gc-identity-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })

	return archive
}

func TestRemoteCollectorMetricsAreLabelFreeAndInventoryTimestampSurvivesPostScanFailures(t *testing.T) {
	t.Parallel()

	fixture := newRemoteGCTestFixture(t, "node-metrics")
	ref := archiveRemoteGCOrphan(t, fixture.archive, "metrics")
	collector := fixture.collector(t, time.Hour)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	registration, err := collector.RegisterMetrics(provider.Meter("remote-gc-metrics-test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, registration.Unregister()) })
	budget := RemoteGCBudget{ScanLimit: 10, DeleteLimit: 10}

	_, err = collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	publishBalanced(t, fixture.store, 1, 1, 1, 1, 1)
	view, err := fixture.store.OpenView(1)
	require.NoError(t, err)
	fixture.now = fixture.now.Add(2 * time.Hour)
	_, err = collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, ErrRemoteGCRootsUnavailable)
	blockedInventory := fixture.now.Unix()

	blocked := readRemoteGCMetrics(t, reader)
	require.Equal(t, int64(1), blocked.integers["balancehistory.remote_gc.inventory.objects"])
	require.Equal(t, int64(1), blocked.integers["balancehistory.remote_gc.queue.objects"])
	require.Equal(t, int64(2*time.Hour/time.Second), blocked.integers["balancehistory.remote_gc.queue.oldest_age"])
	require.Equal(t, int64(1), blocked.integers["balancehistory.remote_gc.blocked.active_view.cycles"])
	require.Zero(t, blocked.integers["balancehistory.remote_gc.delete.failures"])
	require.Zero(t, blocked.integers["balancehistory.remote_gc.deleted.objects"])
	require.Equal(t, blockedInventory, blocked.integers["balancehistory.remote_gc.last_completed_inventory.timestamp"])
	require.Equal(t, uint64(2), blocked.histogramCounts["balancehistory.remote_gc.list.duration"])
	require.Zero(t, blocked.histogramCounts["balancehistory.remote_gc.delete.duration"])
	require.NoError(t, view.Close())
	fixture.now = fixture.now.Add(time.Hour)

	deleteFailure := errors.New("delete unavailable")
	mock := NewMockReclaimer(gomock.NewController(t))
	mock.EXPECT().Namespace().AnyTimes().Return(fixture.archive.Namespace())
	mock.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).Return(balancehistoryarchive.RemoteObjectPage{
		Objects: []balancehistoryarchive.RemoteObject{{
			SHA256: ref.SHA256,
			Size:   int64(ref.Size),
		}},
	}, nil)
	mock.EXPECT().Delete(gomock.Any(), ref.SHA256).Return(deleteFailure)
	tiering := fixture.store.tiering.Load()
	tiering.reclaimer = mock
	_, err = collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, deleteFailure)
	failedDelete := readRemoteGCMetrics(t, reader)
	require.Equal(t, int64(1), failedDelete.integers["balancehistory.remote_gc.delete.failures"])
	require.Equal(t, int64(1), failedDelete.integers["balancehistory.remote_gc.blocked.active_view.cycles"])
	require.Equal(t, fixture.now.Unix(), failedDelete.integers["balancehistory.remote_gc.last_completed_inventory.timestamp"])
	require.Equal(t, uint64(1), failedDelete.histogramCounts["balancehistory.remote_gc.delete.duration"])

	tiering.reclaimer = fixture.archive
	fixture.now = fixture.now.Add(time.Hour)
	deleted, err := collector.Collect(context.Background(), budget)
	require.NoError(t, err)
	require.Equal(t, uint64(1), deleted.DeletedObjects)
	succeeded := readRemoteGCMetrics(t, reader)
	require.Zero(t, succeeded.integers["balancehistory.remote_gc.queue.objects"])
	require.Zero(t, succeeded.integers["balancehistory.remote_gc.queue.oldest_age"])
	require.Equal(t, int64(1), succeeded.integers["balancehistory.remote_gc.deleted.objects"])
	require.Equal(t, int64(ref.Size), succeeded.integers["balancehistory.remote_gc.deleted.bytes"])
	require.Equal(t, int64(1), succeeded.integers["balancehistory.remote_gc.delete.failures"])
	require.Equal(t, fixture.now.Unix(), succeeded.integers["balancehistory.remote_gc.last_completed_inventory.timestamp"])
	require.Equal(t, uint64(4), succeeded.histogramCounts["balancehistory.remote_gc.list.duration"])
	require.Equal(t, uint64(2), succeeded.histogramCounts["balancehistory.remote_gc.delete.duration"])

	listFailure := errors.New("list unavailable")
	mock = NewMockReclaimer(gomock.NewController(t))
	mock.EXPECT().Namespace().AnyTimes().Return(fixture.archive.Namespace())
	mock.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).Return(balancehistoryarchive.RemoteObjectPage{}, listFailure)
	tiering.reclaimer = mock
	_, err = collector.Collect(context.Background(), budget)
	require.ErrorIs(t, err, listFailure)
	failedList := readRemoteGCMetrics(t, reader)
	require.Equal(t, int64(1), failedList.integers["balancehistory.remote_gc.list.failures"])
	require.Equal(t, int64(1), failedList.integers["balancehistory.remote_gc.delete.failures"])
	require.Equal(t, fixture.now.Unix(), failedList.integers["balancehistory.remote_gc.last_completed_inventory.timestamp"])
	require.Equal(t, uint64(5), failedList.histogramCounts["balancehistory.remote_gc.list.duration"])
}

type remoteGCMetricSnapshot struct {
	integers        map[string]int64
	histogramCounts map[string]uint64
}

func readRemoteGCMetrics(t *testing.T, reader *sdkmetric.ManualReader) remoteGCMetricSnapshot {
	t.Helper()

	var resources metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resources))
	snapshot := remoteGCMetricSnapshot{
		integers:        make(map[string]int64),
		histogramCounts: make(map[string]uint64),
	}
	for _, scope := range resources.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			switch data := instrument.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					require.Zero(t, point.Attributes.Len(), instrument.Name)
					snapshot.integers[instrument.Name] = point.Value
				}
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					require.Zero(t, point.Attributes.Len(), instrument.Name)
					snapshot.integers[instrument.Name] = point.Value
				}
			case metricdata.Histogram[float64]:
				for _, point := range data.DataPoints {
					require.Zero(t, point.Attributes.Len(), instrument.Name)
					snapshot.histogramCounts[instrument.Name] = point.Count
				}
			}
		}
	}

	return snapshot
}
