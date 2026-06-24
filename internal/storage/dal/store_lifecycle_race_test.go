package dal_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// newOpenStore opens a store with the default config and registers a cleanup
// that closes it. Shared by every lifecycle/race test in this file so the
// ctx/logger/meter/NewStore boilerplate lives in one place.
func newOpenStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// newClosedStore opens a store and immediately closes it, so every method
// under test sees s.db == nil. Close is idempotent, so the cleanup registered
// by newOpenStore is a harmless no-op.
func newClosedStore(t *testing.T) *dal.Store {
	t.Helper()

	s := newOpenStore(t)
	require.NoError(t, s.Close())

	return s
}

// TestStore_LifecycleMethods_OnClosedStore verifies that the lifecycle methods
// surface ErrStoreClosed once the DB is gone, instead of nil-panicking.
func TestStore_LifecycleMethods_OnClosedStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(t *testing.T, s *dal.Store) error
	}{
		{"Flush", func(_ *testing.T, s *dal.Store) error { return s.Flush() }},
		{"CreateTemporaryCheckpoint", func(_ *testing.T, s *dal.Store) error {
			_, err := s.CreateTemporaryCheckpoint("tmp")

			return err
		}},
		{"CreateQueryCheckpoint", func(_ *testing.T, s *dal.Store) error {
			_, err := s.CreateQueryCheckpoint(1)

			return err
		}},
		{"Checkpoint", func(t *testing.T, s *dal.Store) error { return s.Checkpoint(t.TempDir()) }},
		{"CreateSnapshot", func(_ *testing.T, s *dal.Store) error {
			_, err := s.CreateSnapshot()

			return err
		}},
		{"CompactAll", func(_ *testing.T, s *dal.Store) error { return s.CompactAll() }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newClosedStore(t)
			require.ErrorIs(t, tc.call(t, s), dal.ErrStoreClosed)
		})
	}
}

// TestStore_GetMetrics_OnClosedStore verifies GetMetrics returns nil (not a
// panic) once the DB is gone; the gRPC layer maps nil to Available:false.
func TestStore_GetMetrics_OnClosedStore(t *testing.T) {
	t.Parallel()

	s := newClosedStore(t)
	require.Nil(t, s.GetMetrics())
}

// guardedRaceCalls are the guarded methods that are cheap on an empty store, so
// they can be hammered in a tight loop. Checkpoint/CreateSnapshot do heavier
// filesystem work and are covered by the closed-store table test instead.
func guardedRaceCalls(s *dal.Store) []func() {
	return []func(){
		func() { _ = s.Flush() },
		func() { _ = s.CompactAll() },
		func() { _ = s.GetMetrics() },
	}
}

// raceGuardedReaders launches one goroutine per guarded call, each looping 200
// times. Every goroutine blocks on start before its loop so the dbMu writer
// (Close/RestoreCheckpoint) can be released at the same instant and actually
// interleave with an in-progress call; without the gate the scheduler may drain
// the readers before the writer runs and -race never sees the s.db overlap.
func raceGuardedReaders(wg *sync.WaitGroup, start <-chan struct{}, calls []func()) {
	for _, call := range calls {
		wg.Add(1)

		go func(fn func()) {
			defer wg.Done()

			<-start

			for range 200 {
				fn()
			}
		}(call)
	}
}

// TestStore_GuardedMethods_RaceWithClose runs the guarded methods concurrently
// with Close. Run under -race: it catches any unsynchronized s.db access in the
// getDB/dbMu pattern. After Close the methods return ErrStoreClosed/nil rather
// than panic, so the calls intentionally ignore their results.
func TestStore_GuardedMethods_RaceWithClose(t *testing.T) {
	t.Parallel()

	s := newOpenStore(t)

	start := make(chan struct{})

	var wg sync.WaitGroup

	raceGuardedReaders(&wg, start, guardedRaceCalls(s))

	// Single Close racing the readers above; readers tolerate ErrStoreClosed/nil.
	wg.Go(func() {
		<-start

		_ = s.Close()
	})

	close(start)
	wg.Wait()
}

// TestStore_GuardedMethods_RaceWithRestoreCheckpoint runs the guarded methods
// concurrently with RestoreCheckpoint, the other dbMu writer. RestoreCheckpoint
// is the dangerous live follower-restore swap (s.db = nil then s.db = newDB
// under Lock; store.go), which Close alone does not exercise. Run under -race:
// it catches any unsynchronized s.db access across that swap. Readers tolerate
// ErrStoreClosed/nil and the restore tolerates transient errors; all results
// are intentionally ignored -- the point is the race detector, not behaviour.
func TestStore_GuardedMethods_RaceWithRestoreCheckpoint(t *testing.T) {
	t.Parallel()

	s := newOpenStore(t)

	// Seed a key and take a committed checkpoint to restore. The checkpoint
	// directory survives repeated restores, so RestoreCheckpoint(checkpointID)
	// can be replayed in the loop below.
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("seed"), []byte("value")))
	require.NoError(t, batch.Commit())

	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)

	start := make(chan struct{})

	var wg sync.WaitGroup

	raceGuardedReaders(&wg, start, guardedRaceCalls(s))

	// RestoreCheckpoint is heavy (rename + hard-link + pebble.Open), so it
	// loops far fewer times than the readers. It is the dbMu writer the
	// readers must stay synchronized against.
	wg.Go(func() {
		<-start

		for range 10 {
			_ = s.RestoreCheckpoint(checkpointID)
		}
	})

	close(start)
	wg.Wait()
}

// TestStore_CreateQueryCheckpoint_ErrorOnExistingDir covers the checkpoint-failure
// branch: the second checkpoint for the same id targets an already-existing
// directory, which Pebble rejects, so CreateQueryCheckpoint returns an error.
func TestStore_CreateQueryCheckpoint_ErrorOnExistingDir(t *testing.T) {
	t.Parallel()

	s := newOpenStore(t)

	_, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)

	_, err = s.CreateQueryCheckpoint(1)
	require.Error(t, err)
}
