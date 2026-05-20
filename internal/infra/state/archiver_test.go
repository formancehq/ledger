package state

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger-v3-poc/internal/infra/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// mockColdStorage is a test-only in-memory implementation of coldstorage.ColdStorage.
type mockColdStorage struct {
	mu       sync.Mutex
	archives map[string][]byte // bucketID/periodID -> data
}

func newMockColdStorage() *mockColdStorage {
	return &mockColdStorage{archives: make(map[string][]byte)}
}

func (m *mockColdStorage) Archive(_ context.Context, bucketID string, periodID uint64, data io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := mockArchiveKey(bucketID, periodID)

	var buf []byte
	if data != nil {
		var err error

		buf, err = io.ReadAll(data)
		if err != nil {
			return err
		}
	}

	m.archives[key] = buf

	return nil
}

func (m *mockColdStorage) Exists(_ context.Context, bucketID string, periodID uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := mockArchiveKey(bucketID, periodID)
	_, ok := m.archives[key]

	return ok, nil
}

func (m *mockColdStorage) Fetch(_ context.Context, bucketID string, periodID uint64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := mockArchiveKey(bucketID, periodID)

	data, ok := m.archives[key]
	if !ok {
		return nil, fmt.Errorf("archive %s not found", key)
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func mockArchiveKey(bucketID string, periodID uint64) string {
	return fmt.Sprintf("%s/%d", bucketID, periodID)
}

func TestArchiverStartStop(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	archiveReqCh := make(chan ArchiveRequest, 1)
	cs := newMockColdStorage()

	a := NewArchiver(logger, nil, cs, archiveReqCh, func(periodID uint64) {}, func() bool { return true }, "test-bucket")
	a.Start()
	a.Stop()
	// No deadlock or panic means success
}

func TestArchiverArchivesAndProposes(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	// Create a real store with some data so buildArchive works
	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	// Store a log so IterateColdKVPairs finds something
	batch := dataStore.NewBatch()
	require.NoError(t, AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload:  &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreateLedgerLog{Name: "test", CreatedAt: commonpb.NewTimestamp(libtime.Now())}}},
	}}))
	require.NoError(t, batch.Commit())

	cs := newMockColdStorage()
	archiveReqCh := make(chan ArchiveRequest, 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		dataStore,
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true },
		"test-bucket",
	)
	a.Start()

	// Send an archive request
	archiveReqCh <- ArchiveRequest{
		PeriodID:      1,
		StartSequence: 1,
		CloseSequence: 1,
	}

	// Wait for the archive to complete
	require.Eventually(t, func() bool {
		return proposedPeriodID.Load() == 1
	}, 5*time.Second, 50*time.Millisecond, "archiver should propose ConfirmArchivePeriod")

	a.Stop()

	// Verify the archive exists in cold storage
	exists, err := cs.Exists(context.Background(), "test-bucket", 1)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestArchiverAlreadyArchivedLeaderProposes(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newMockColdStorage()
	// Pre-populate: archive already exists
	require.NoError(t, cs.Archive(context.Background(), "test-bucket", 5, nil))

	archiveReqCh := make(chan ArchiveRequest, 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		nil, // no dataStore needed since archive already exists
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true }, // is leader
		"test-bucket",
	)
	a.Start()

	// Send request for already-archived period
	archiveReqCh <- ArchiveRequest{
		PeriodID:      5,
		StartSequence: 1,
		CloseSequence: 10,
	}

	// Leader should still propose ConfirmArchivePeriod (crash recovery)
	require.Eventually(t, func() bool {
		return proposedPeriodID.Load() == 5
	}, 5*time.Second, 50*time.Millisecond, "leader should propose for already-archived period")

	a.Stop()
}

func TestArchiverAlreadyArchivedFollowerDoesNotPropose(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newMockColdStorage()
	// Pre-populate: archive already exists
	require.NoError(t, cs.Archive(context.Background(), "test-bucket", 7, nil))

	archiveReqCh := make(chan ArchiveRequest, 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		nil,
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return false }, // not leader
		"test-bucket",
	)
	a.Start()

	// Send request
	archiveReqCh <- ArchiveRequest{
		PeriodID:      7,
		StartSequence: 1,
		CloseSequence: 10,
	}

	// Follower should not propose
	require.Never(t, func() bool { return proposedPeriodID.Load() > 0 }, 200*time.Millisecond, 10*time.Millisecond, "follower should not propose")

	a.Stop()
}

func TestArchiverNonLeaderRetries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	cs := newMockColdStorage()
	archiveReqCh := make(chan ArchiveRequest, 1)

	var (
		proposedPeriodID atomic.Uint64
		isLeader         atomic.Bool
	)

	isLeader.Store(false)

	a := NewArchiver(
		logger,
		dataStore,
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		isLeader.Load,
		"test-bucket",
	)
	a.Start()

	// Send request while not leader
	archiveReqCh <- ArchiveRequest{
		PeriodID:      3,
		StartSequence: 1,
		CloseSequence: 1,
	}

	// Should not have proposed yet
	require.Never(t, func() bool { return proposedPeriodID.Load() > 0 }, 200*time.Millisecond, 10*time.Millisecond, "non-leader should not propose yet")

	// Become leader - archiver should eventually succeed
	isLeader.Store(true)

	// Store a log so buildArchive has data
	batch := dataStore.NewBatch()
	require.NoError(t, AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload:  &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreateLedgerLog{Name: "test", CreatedAt: commonpb.NewTimestamp(libtime.Now())}}},
	}}))
	require.NoError(t, batch.Commit())

	require.Eventually(t, func() bool {
		return proposedPeriodID.Load() == 3
	}, 10*time.Second, 100*time.Millisecond, "archiver should eventually succeed after becoming leader")

	a.Stop()
}

func TestArchiverSSTRoundtrip(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	// Store a log so IterateColdKVPairs finds something
	batch := dataStore.NewBatch()
	require.NoError(t, AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Name: "test-ledger", CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	}}))
	require.NoError(t, batch.Commit())

	cs := newMockColdStorage()
	archiveReqCh := make(chan ArchiveRequest, 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		dataStore,
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true },
		"test-bucket",
	)
	a.Start()

	archiveReqCh <- ArchiveRequest{
		PeriodID:      1,
		StartSequence: 1,
		CloseSequence: 1,
	}

	require.Eventually(t, func() bool {
		return proposedPeriodID.Load() == 1
	}, 5*time.Second, 50*time.Millisecond, "archiver should propose ConfirmArchivePeriod")

	a.Stop()

	// Now read the SST back via ColdReader and verify the log is readable
	cacheDir := t.TempDir()
	coldReader := coldstorage.NewColdReader(cs, "test-bucket", cacheDir, 4, 0, logger)
	t.Cleanup(func() { _ = coldReader.Close() })

	pebbleReader, err := coldReader.GetReader(ctx, 1)
	require.NoError(t, err)

	// Verify the log is readable from the ingested SST
	log, err := query.ReadLogBySequence(ctx, pebbleReader, 1)
	require.NoError(t, err, "log should be readable from cold storage")
	require.NotNil(t, log, "log should not be nil")
	require.Equal(t, uint64(1), log.GetSequence())

	// Non-existent log should return nil
	log, err = query.ReadLogBySequence(ctx, pebbleReader, 999)
	require.NoError(t, err)
	require.Nil(t, log)
}
