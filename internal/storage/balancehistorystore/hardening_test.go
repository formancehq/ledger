package balancehistorystore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

func openTestStoreAt(t *testing.T, dir string) *Store {
	t.Helper()

	store, err := New(dir, logging.NopZap(), DefaultConfig())
	require.NoError(t, err)

	return store
}

func TestStoreReadinessFailureRepairAndQuarantineAreFailClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)

	_, err := store.OpenView(0)
	var building *ErrBuilding
	require.ErrorAs(t, err, &building)

	_, err = store.Publish(Publication{
		Effects:  []balancehistory.Effect{inputEffect(1, 1, 10, 10, "default", "a", 1)},
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1},
	})
	require.NoError(t, err)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &building)

	_, err = store.Publish(Publication{
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
	})
	require.NoError(t, err)
	view, err := store.OpenView(1)
	require.NoError(t, err)

	require.NoError(t, store.MarkSourceMissing("archive chapter 1 is unavailable"))
	_, err = view.ReadVolumes("default", TemporalityEffective, 10, nil)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &missing)
	require.NoError(t, view.Close())

	require.NoError(t, store.Close())
	store = openTestStoreAt(t, dir)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &missing)
	require.NoError(t, store.ClearFailure(1, 1))
	repaired, err := store.OpenView(1)
	require.NoError(t, err)
	require.NoError(t, repaired.Close())

	require.NoError(t, store.Quarantine("source replay structure differs"))
	_, err = store.OpenView(1)
	var quarantined *ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	var corrupt *ErrCorrupt
	require.ErrorAs(t, err, &corrupt)
	require.ErrorAs(t, store.ClearFailure(0, 0), &quarantined)

	require.NoError(t, store.Close())
	store = openTestStoreAt(t, dir)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &quarantined)
	require.NoError(t, store.Reset())
	_, err = store.OpenView(0)
	require.ErrorAs(t, err, &building)
	require.NoError(t, store.Close())
}

func TestWaitForWatermarkPreservesCancellationAndReportsSourceMissing(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.WaitForLogWatermark(ctx, 12)
	require.ErrorIs(t, err, context.Canceled)

	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer deadlineCancel()
	require.ErrorIs(t, store.WaitForLogWatermark(deadlineCtx, 12), context.DeadlineExceeded)

	require.NoError(t, store.MarkSourceMissing("verified source gap"))
	err = store.WaitForLogWatermark(context.Background(), 12)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
}

func TestQuarantinedStoreRebuildNeverServesPartialHistory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)
	publishBalanced(t, store, 1, 1, 1, 1, 1)
	require.NoError(t, store.Quarantine("malformed segment record"))
	require.NoError(t, store.ResetForRebuild())

	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.Version)
	_, err = store.OpenView(0)
	var quarantined *ErrQuarantined
	require.ErrorAs(t, err, &quarantined)

	_, err = store.Publish(Publication{
		Effects:  []balancehistory.Effect{inputEffect(1, 1, 1, 1, "default", "a", 4)},
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1},
	})
	require.NoError(t, err)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &quarantined)
	var building *ErrBuilding
	require.ErrorAs(t, store.CompleteRebuild(1, 1), &building)

	// REBUILDING survives process restart and still permits the builder to
	// inspect its partial durable cursor without reopening reads.
	require.NoError(t, store.Close())
	store = openTestStoreAt(t, dir)
	manifest, err = store.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.AuditWatermark)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &quarantined)

	_, err = store.Publish(Publication{
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
	})
	require.NoError(t, err)
	require.ErrorContains(t, store.CompleteRebuild(2, 1), "behind required head")
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &quarantined)
	require.NoError(t, store.CompleteRebuild(1, 1))
	view, err := store.OpenView(1)
	require.NoError(t, err)
	volumes, err := view.ReadVolumes("default", TemporalityEffective, 1, []string{"a"})
	require.NoError(t, err)
	require.Equal(t, "4", volumes[0].Input.String())
	require.NoError(t, view.Close())
	require.NoError(t, store.Close())
}

func TestSourceRepairResetPreservesFailureUntilExplicitCompletion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)
	require.NoError(t, store.ResetForConfiguration([]string{"default"}))
	publishBalanced(t, store, 1, 1, 1, 1, 1)
	require.NoError(t, store.CompleteRebuild(1, 1))
	require.NoError(t, store.MarkSourceMissing("archive range is missing"))
	require.NoError(t, store.ResetForSourceRepair())

	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.Version)
	require.Equal(t, []string{"default"}, manifest.Ledgers)
	_, err = store.OpenView(0)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)

	_, err = store.Publish(Publication{
		Effects:  []balancehistory.Effect{inputEffect(1, 1, 1, 1, "default", "a", 4)},
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
	})
	require.NoError(t, err)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &missing)

	// Both the repaired manifest and its fail-closed marker survive restart.
	require.NoError(t, store.SyncWAL())
	require.NoError(t, store.Close())
	store = openTestStoreAt(t, dir)
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &missing)

	require.ErrorContains(t, store.ClearFailure(2, 1), "behind required head")
	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &missing)
	require.NoError(t, store.ClearFailure(1, 1))
	view, err := store.OpenView(1)
	require.NoError(t, err)
	volumes, err := view.ReadVolumes("default", TemporalityEffective, 1, []string{"a"})
	require.NoError(t, err)
	require.Equal(t, "4", volumes[0].Input.String())
	require.NoError(t, view.Close())
	require.NoError(t, store.Close())
}

func TestTornManifestPointerIsQuarantinedOnOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)
	publishBalanced(t, store, 1, 1, 1, 1, 1)
	var missingVersion [8]byte
	binary.BigEndian.PutUint64(missingVersion[:], 999)
	require.NoError(t, store.db.Set(latestManifestKey(), missingVersion[:], pebble.Sync))
	require.NoError(t, store.Close())

	store = openTestStoreAt(t, dir)
	_, err := store.Manifest()
	var quarantined *ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	require.NoError(t, store.ResetForRebuild())
	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.Version)
	require.NoError(t, store.Close())
}

func TestMalformedAndUnsupportedManifestsReopenAsQuarantined(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(t *testing.T, store *Store, manifest Manifest)
	}{
		{
			name: "malformed json",
			mutate: func(t *testing.T, store *Store, manifest Manifest) {
				t.Helper()
				require.NoError(t, store.db.Set(manifestKey(manifest.Version), []byte("{"), pebble.Sync))
			},
		},
		{
			name: "unsupported format",
			mutate: func(t *testing.T, store *Store, manifest Manifest) {
				t.Helper()
				manifest.FormatVersion = formatVersion + 1
				encoded, err := encodeManifest(manifest)
				require.NoError(t, err)
				require.NoError(t, store.db.Set(manifestKey(manifest.Version), encoded, pebble.Sync))
			},
		},
		{
			name: "unsupported reducer",
			mutate: func(t *testing.T, store *Store, manifest Manifest) {
				t.Helper()
				manifest.ReducerVersion = reducerVersion + 1
				encoded, err := encodeManifest(manifest)
				require.NoError(t, err)
				require.NoError(t, store.db.Set(manifestKey(manifest.Version), encoded, pebble.Sync))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			store := openTestStoreAt(t, dir)
			manifest := publishBalanced(t, store, 1, 1, 1, 1, 1)
			test.mutate(t, store, manifest)
			require.NoError(t, store.Close())

			store = openTestStoreAt(t, dir)
			_, err := store.OpenView(1)
			var quarantined *ErrQuarantined
			require.ErrorAs(t, err, &quarantined)
			var corrupt *ErrCorrupt
			require.ErrorAs(t, err, &corrupt)
			require.NoError(t, store.Close())
		})
	}
}

func TestAbortedPublicationAndOrphanRunDoNotAdvanceManifest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)
	first := publishBalanced(t, store, 1, 1, 10, 10, 2)

	// An uncommitted Pebble batch models process death before the atomic
	// publication commit. Neither the pointer nor any staged bytes survive.
	aborted := store.db.NewBatch()
	var bogusVersion [8]byte
	binary.BigEndian.PutUint64(bogusVersion[:], first.Version+1)
	require.NoError(t, aborted.Set(latestManifestKey(), bogusVersion[:], nil))
	require.NoError(t, aborted.Set(manifestKey(first.Version+1), []byte("partial"), nil))
	require.NoError(t, aborted.Close())

	// An orphan run models the stricter data-before-manifest protocol crashing
	// after durable run bytes but before pointer publication. It must be ignored
	// and safely reclaimable after restart.
	orphanEffects := []balancehistory.Effect{inputEffect(2, 2, 20, 20, "default", "orphan", 9)}
	records, entries, identities, err := buildRunRecords(99, orphanEffects)
	require.NoError(t, err)
	orphanRef := SegmentRef{
		ID: 99, Level: 0, FirstAuditSequence: 2, LastAuditSequence: 2,
		MaxLogSequence: 2, EntryCount: entries, IdentityCount: identities,
	}
	encodedRef, err := json.Marshal(orphanRef)
	require.NoError(t, err)
	orphanBatch := store.db.NewBatch()
	for _, record := range records {
		require.NoError(t, orphanBatch.Set(record.key, record.value, nil))
	}
	require.NoError(t, orphanBatch.Set(runMetaKey(99), encodedRef, nil))
	require.NoError(t, orphanBatch.Commit(pebble.Sync))
	require.NoError(t, orphanBatch.Close())
	require.NoError(t, store.Close())

	store = openTestStoreAt(t, dir)
	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Equal(t, first.Version, manifest.Version)
	require.Len(t, manifest.Segments, 1)

	view, err := store.OpenView(1)
	require.NoError(t, err)
	volumes, err := view.ReadVolumes("default", TemporalityEffective, 100, nil)
	require.NoError(t, err)
	require.Len(t, volumes, 2)
	require.NoError(t, view.Close())

	collected, err := store.CollectGarbage()
	require.NoError(t, err)
	require.True(t, collected)
	_, closer, err := store.db.Get(runMetaKey(99))
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.NoError(t, store.Close())
}

func TestDurablePrefixCanReplayAnAsynchronousPublicationSuffix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)
	first := publishBalanced(t, store, 1, 1, 10, 10, 2)
	require.NoError(t, store.SyncWAL())

	// Capture the explicitly synced prefix, then publish a NoSync suffix. A
	// power loss is allowed to recover either prefix; restoring this checkpoint
	// deterministically exercises the lost-suffix branch.
	durablePrefix := filepath.Join(dir, "durable-prefix")
	require.NoError(t, store.db.Checkpoint(durablePrefix))
	publishBalanced(t, store, 2, 2, 20, 20, 3)
	require.NoError(t, store.Close())

	dbPath := filepath.Join(dir, "balancehistorydb")
	lostSuffix := filepath.Join(dir, "lost-unsynced-suffix")
	require.NoError(t, os.Rename(dbPath, lostSuffix))
	require.NoError(t, os.Rename(durablePrefix, dbPath))

	store = openTestStoreAt(t, dir)
	recovered, err := store.Manifest()
	require.NoError(t, err)
	require.Equal(t, first.AuditWatermark, recovered.AuditWatermark)

	publishBalanced(t, store, 2, 2, 20, 20, 3)
	view, err := store.OpenView(2)
	require.NoError(t, err)
	volumes, err := view.ReadVolumes("default", TemporalityEffective, 20, []string{"assets:cash"})
	require.NoError(t, err)
	require.Equal(t, "5", volumes[0].Input.String())
	require.NoError(t, view.Close())
	require.NoError(t, store.SyncWAL())
	require.NoError(t, store.Close())
}

func TestCompactionGarbageCollectionHonorsViewLeases(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	for sequence := uint64(1); sequence <= 2; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence, sequence, sequence)
	}

	pinned, err := store.OpenView(2)
	require.NoError(t, err)
	oldRuns := pinned.Manifest().Segments
	require.Len(t, oldRuns, 2)

	compacted, err := store.Compact(2)
	require.NoError(t, err)
	require.True(t, compacted)
	for _, run := range oldRuns {
		_, closer, err := store.db.Get(runMetaKey(run.ID))
		require.NoError(t, err)
		require.NoError(t, closer.Close())
	}
	volumes, err := pinned.ReadVolumes("default", TemporalityEffective, 2, []string{"assets:cash"})
	require.NoError(t, err)
	require.Equal(t, "3", volumes[0].Input.String())

	collected, err := store.CollectGarbage()
	require.NoError(t, err)
	// The unleased reservation manifest is collectible, while the pinned
	// pre-compaction manifest and its runs must remain intact.
	require.True(t, collected)
	require.NoError(t, pinned.Close())
	collected, err = store.CollectGarbage()
	require.NoError(t, err)
	require.True(t, collected)
	for _, run := range oldRuns {
		_, closer, err := store.db.Get(runMetaKey(run.ID))
		if closer != nil {
			require.NoError(t, closer.Close())
		}
		require.ErrorIs(t, err, pebble.ErrNotFound)
	}
	require.NoError(t, store.Verify())
}

func TestCompleteRebuildScansEveryStoredRecordBeforeReopening(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	require.NoError(t, store.ResetForConfiguration([]string{"default"}))
	publishBalanced(t, store, 1, 1, 1, 1, 1)

	prefix := []byte{prefixRunData}
	iter, err := store.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: []byte{prefixRunData + 1}})
	require.NoError(t, err)
	require.True(t, iter.First())
	key := append([]byte(nil), iter.Key()...)
	require.NoError(t, iter.Close())
	require.NoError(t, store.db.Set(key, []byte{1}, pebble.Sync))

	err = store.CompleteRebuild(1, 1)
	var corrupt *ErrCorrupt
	require.ErrorAs(t, err, &corrupt)
	_, err = store.OpenView(1)
	var quarantined *ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
}

func TestResetInvalidatesPinnedViews(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	publishBalanced(t, store, 1, 1, 1, 1, 1)
	view, err := store.OpenView(1)
	require.NoError(t, err)
	require.NoError(t, store.Reset())

	_, err = view.ReadVolumes("default", TemporalityEffective, 1, nil)
	var missing *ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	require.NoError(t, view.Close())
}

func TestReadVolumesByPrefixUsesHistoricalAccountKeyRange(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	accounts := []string{"assets:a", "assets:ab", "assets:b", "liabilities:a", "assets:\x00binary"}
	effects := make([]balancehistory.Effect, 0, len(accounts))
	for _, account := range accounts {
		effects = append(effects, inputEffect(1, 1, 1, 1, "default", account, 1))
	}
	_, err := store.Publish(Publication{
		Effects: effects, Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
	})
	require.NoError(t, err)
	view, err := store.OpenView(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	volumes, err := view.ReadVolumesByPrefix("default", TemporalityEffective, 1, "assets:a")
	require.NoError(t, err)
	require.Equal(t, []string{"assets:a", "assets:ab"}, volumeAccounts(volumes))
	volumes, err = view.ReadVolumesByPrefix("default", TemporalityEffective, 1, "assets:")
	require.NoError(t, err)
	require.Equal(t, []string{"assets:\x00binary", "assets:a", "assets:ab", "assets:b"}, volumeAccounts(volumes))
	require.NoError(t, store.Verify())
}

func volumeAccounts(volumes []Volume) []string {
	accounts := make([]string, len(volumes))
	for index, volume := range volumes {
		accounts[index] = volume.Account
	}

	return accounts
}

func TestConcurrentViewsCompactionAndGarbageCollection(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	for sequence := uint64(1); sequence <= 8; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence, sequence, 1)
	}

	const readerCount = 8
	errorsChannel := make(chan error, readerCount+1)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range readerCount {
		wait.Go(func() {
			<-start
			for range 25 {
				view, err := store.OpenView(8)
				if err != nil {
					errorsChannel <- err

					return
				}
				volumes, err := view.ReadVolumes("default", TemporalityEffective, 8, []string{"assets:cash"})
				if err == nil && (len(volumes) != 1 || volumes[0].Input.String() != "8") {
					err = errors.New("concurrent view returned an incomplete total")
				}
				closeErr := view.Close()
				if err != nil {
					errorsChannel <- err

					return
				}
				if closeErr != nil {
					errorsChannel <- closeErr

					return
				}
			}
		})
	}
	wait.Go(func() {
		<-start
		for range 8 {
			if _, err := store.Compact(2); err != nil {
				errorsChannel <- err

				return
			}
			if _, err := store.CollectGarbage(); err != nil {
				errorsChannel <- err

				return
			}
		}
	})
	close(start)
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		require.NoError(t, err)
	}
	require.NoError(t, store.Verify())
}
