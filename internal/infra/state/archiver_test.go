package state

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// mockArchive holds the bytes of an archive plus its persisted SHA-256 — the
// two-step state that real backends keep (object body + checksum sidecar /
// user-metadata). `data` and `checksum` can be set independently to simulate
// crashed-mid-upload scenarios.
type mockArchive struct {
	data     []byte
	checksum []byte
}

// mockColdStorage is a test-only in-memory implementation of coldstorage.ColdStorage.
type mockColdStorage struct {
	mu       sync.Mutex
	archives map[string]*mockArchive

	archiveCalls atomic.Int32 // count of successful Archive() invocations
}

func newMockColdStorage() *mockColdStorage {
	return &mockColdStorage{archives: make(map[string]*mockArchive)}
}

func (m *mockColdStorage) Archive(_ context.Context, bucketID string, periodID uint64, data io.Reader, sha256 []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(sha256) != coldstorage.ChecksumLength {
		return fmt.Errorf("mock: invalid checksum length %d", len(sha256))
	}

	key := mockArchiveKey(bucketID, periodID)

	var buf []byte
	if data != nil {
		var err error

		buf, err = io.ReadAll(data)
		if err != nil {
			return err
		}
	}

	checksumCopy := make([]byte, len(sha256))
	copy(checksumCopy, sha256)

	m.archives[key] = &mockArchive{data: buf, checksum: checksumCopy}
	m.archiveCalls.Add(1)

	return nil
}

func (m *mockColdStorage) Exists(_ context.Context, bucketID string, periodID uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.archives[mockArchiveKey(bucketID, periodID)]
	if !ok {
		return false, nil
	}

	// Match the real backends: an archive is "fully committed" only when
	// both the data and its persisted checksum are present.
	return a.data != nil && a.checksum != nil, nil
}

func (m *mockColdStorage) ExpectedChecksum(_ context.Context, bucketID string, periodID uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.archives[mockArchiveKey(bucketID, periodID)]
	if !ok || a.checksum == nil {
		return nil, coldstorage.ErrChecksumNotFound
	}

	out := make([]byte, len(a.checksum))
	copy(out, a.checksum)

	return out, nil
}

func (m *mockColdStorage) Checksum(_ context.Context, bucketID string, periodID uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.archives[mockArchiveKey(bucketID, periodID)]
	if !ok || a.data == nil {
		return nil, fmt.Errorf("archive %s/%d not found", bucketID, periodID)
	}

	return coldstorage.ComputeSHA256(bytes.NewReader(a.data))
}

func (m *mockColdStorage) Fetch(_ context.Context, bucketID string, periodID uint64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.archives[mockArchiveKey(bucketID, periodID)]
	if !ok || a.data == nil {
		return nil, fmt.Errorf("archive %s/%d not found", bucketID, periodID)
	}

	return io.NopCloser(bytes.NewReader(a.data)), nil
}

// seed inserts a synthetic archive (used to reproduce crash-recovery state in
// tests). Pass checksum=nil to simulate a data-only partial upload.
func (m *mockColdStorage) seed(bucketID string, periodID uint64, data, checksum []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.archives[mockArchiveKey(bucketID, periodID)] = &mockArchive{
		data:     append([]byte(nil), data...),
		checksum: append([]byte(nil), checksum...),
	}
}

func mockArchiveKey(bucketID string, periodID uint64) string {
	return fmt.Sprintf("%s/%d", bucketID, periodID)
}

func TestArchiverStartStop(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)
	cs := newMockColdStorage()

	a := NewArchiver(logger, nil, cs, archiveReqCh, func(periodID uint64) {}, func() bool { return true }, "test-bucket", func(<-chan struct{}) {})
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
		Payload:  &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "test", CreatedAt: commonpb.NewTimestamp(libtime.Now())}}},
	}}))
	require.NoError(t, batch.Commit())

	cs := newMockColdStorage()
	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		dataStore,
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true },
		"test-bucket",
		func(<-chan struct{}) {},
	)
	a.Start()

	// Send an archive request
	archiveReqCh.TrySend(ArchiveRequest{
		PeriodID:      1,
		StartSequence: 1,
		CloseSequence: 1,
	}, "test")

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
	// Pre-populate: archive already exists with a consistent checksum.
	data := []byte("existing archive contents")
	expected, err := coldstorage.ComputeSHA256(bytes.NewReader(data))
	require.NoError(t, err)
	cs.seed("test-bucket", 5, data, expected)

	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		nil, // no dataStore needed since archive already exists
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true }, // is leader
		"test-bucket",
		func(<-chan struct{}) {},
	)
	a.Start()

	// Send request for already-archived period
	archiveReqCh.TrySend(ArchiveRequest{
		PeriodID:      5,
		StartSequence: 1,
		CloseSequence: 10,
	}, "test")

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
	// Pre-populate: archive already exists with a consistent checksum.
	data := []byte("existing archive contents")
	expected, err := coldstorage.ComputeSHA256(bytes.NewReader(data))
	require.NoError(t, err)
	cs.seed("test-bucket", 7, data, expected)

	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		nil,
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return false }, // not leader
		"test-bucket",
		func(<-chan struct{}) {},
	)
	a.Start()

	// Send request
	archiveReqCh.TrySend(ArchiveRequest{
		PeriodID:      7,
		StartSequence: 1,
		CloseSequence: 10,
	}, "test")

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
	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

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
		func(<-chan struct{}) {},
	)
	a.Start()

	// Send request while not leader
	archiveReqCh.TrySend(ArchiveRequest{
		PeriodID:      3,
		StartSequence: 1,
		CloseSequence: 1,
	}, "test")

	// Should not have proposed yet
	require.Never(t, func() bool { return proposedPeriodID.Load() > 0 }, 200*time.Millisecond, 10*time.Millisecond, "non-leader should not propose yet")

	// Become leader - archiver should eventually succeed
	isLeader.Store(true)

	// Store a log so buildArchive has data
	batch := dataStore.NewBatch()
	require.NoError(t, AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload:  &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "test", CreatedAt: commonpb.NewTimestamp(libtime.Now())}}},
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
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name: "test-ledger", CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	}}))
	require.NoError(t, batch.Commit())

	cs := newMockColdStorage()
	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(
		logger,
		dataStore,
		cs,
		archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true },
		"test-bucket",
		func(<-chan struct{}) {},
	)
	a.Start()

	archiveReqCh.TrySend(ArchiveRequest{
		PeriodID:      1,
		StartSequence: 1,
		CloseSequence: 1,
	}, "test")

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

// archiveRequestForTest builds an ArchiveRequest and a dataStore seeded with
// one log, suitable for the upload path (buildSSTArchive needs cold KV pairs).
func archiveRequestForTest(t *testing.T) (*dal.Store, ArchiveRequest) {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	batch := dataStore.NewBatch()
	require.NoError(t, AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name:      "test-ledger",
				CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	}}))
	require.NoError(t, batch.Commit())

	return dataStore, ArchiveRequest{PeriodID: 42, StartSequence: 1, CloseSequence: 1}
}

func TestArchiver_FreshUploadPersistsChecksum(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	dataStore, req := archiveRequestForTest(t)

	cs := newMockColdStorage()
	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(logger, dataStore, cs, archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true }, "test-bucket", func(<-chan struct{}) {})
	a.Start()
	t.Cleanup(a.Stop)

	archiveReqCh.TrySend(req, "test")

	require.Eventually(t, func() bool {
		return proposedPeriodID.Load() == req.PeriodID
	}, 5*time.Second, 50*time.Millisecond, "should propose after fresh upload")

	expected, err := cs.ExpectedChecksum(ctx, "test-bucket", req.PeriodID)
	require.NoError(t, err, "checksum must be persisted with the archive")
	require.Len(t, expected, coldstorage.ChecksumLength)

	current, err := cs.Checksum(ctx, "test-bucket", req.PeriodID)
	require.NoError(t, err)
	require.Equal(t, expected, current, "persisted checksum must match the data it was uploaded with")
}

func TestArchiver_CrashRecoveryWithValidArchive(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newMockColdStorage()
	data := []byte("intact archive bytes")
	expected, err := coldstorage.ComputeSHA256(bytes.NewReader(data))
	require.NoError(t, err)
	cs.seed("test-bucket", 11, data, expected)

	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(logger, nil, cs, archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true }, "test-bucket", func(<-chan struct{}) {})
	a.Start()
	t.Cleanup(a.Stop)

	archiveReqCh.TrySend(ArchiveRequest{PeriodID: 11, StartSequence: 1, CloseSequence: 1}, "test")

	require.Eventually(t, func() bool {
		return proposedPeriodID.Load() == 11
	}, 5*time.Second, 50*time.Millisecond, "integrity-verified archive should be proposed")

	require.EqualValues(t, 0, cs.archiveCalls.Load(),
		"crash-recovery must NOT re-upload when integrity check passes")
}

func TestArchiver_CrashRecoveryWithCorruptArchive(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newMockColdStorage()
	// Seed with a checksum that does NOT match the data — simulates bit rot
	// or a truncated upload that was never re-pushed.
	originalChecksum, err := coldstorage.ComputeSHA256(bytes.NewReader([]byte("intended bytes")))
	require.NoError(t, err)
	cs.seed("test-bucket", 13, []byte("corrupted bytes"), originalChecksum)

	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(logger, nil, cs, archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true }, "test-bucket", func(<-chan struct{}) {})
	a.Start()
	t.Cleanup(a.Stop)

	archiveReqCh.TrySend(ArchiveRequest{PeriodID: 13, StartSequence: 1, CloseSequence: 1}, "test")

	// Propose must NEVER happen for a corrupt archive.
	require.Never(t, func() bool { return proposedPeriodID.Load() > 0 },
		400*time.Millisecond, 25*time.Millisecond,
		"corrupt archive must NOT be confirmed")
	require.EqualValues(t, 0, cs.archiveCalls.Load(),
		"crash-recovery must not re-upload on integrity failure — escalate via logged error")
}

func TestArchiver_LegacyDataOnlyTriggersReupload(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	dataStore, req := archiveRequestForTest(t)

	cs := newMockColdStorage()
	// Legacy state (pre-PR): data was uploaded but no checksum sidecar was
	// ever written. Exists() must report false so the leader re-uploads.
	cs.seed("test-bucket", req.PeriodID, []byte("legacy bytes"), nil)

	archiveReqCh := worker.NewChannel[ArchiveRequest](logger, "test-archive", 1)

	var proposedPeriodID atomic.Uint64

	a := NewArchiver(logger, dataStore, cs, archiveReqCh,
		func(periodID uint64) { proposedPeriodID.Store(periodID) },
		func() bool { return true }, "test-bucket", func(<-chan struct{}) {})
	a.Start()
	t.Cleanup(a.Stop)

	archiveReqCh.TrySend(req, "test")

	require.Eventually(t, func() bool {
		return proposedPeriodID.Load() == req.PeriodID
	}, 5*time.Second, 50*time.Millisecond, "leader must re-upload legacy data-only archives")

	// After re-upload, the checksum is now present.
	persistedChecksum, err := cs.ExpectedChecksum(ctx, "test-bucket", req.PeriodID)
	require.NoError(t, err)
	require.Len(t, persistedChecksum, coldstorage.ChecksumLength)

	require.EqualValues(t, 1, cs.archiveCalls.Load(),
		"legacy state must trigger exactly one re-upload")
}

func TestArchiver_BuildSSTIsDeterministic(t *testing.T) {
	t.Parallel()

	dataStore, req := archiveRequestForTest(t)

	a := &Archiver{dataStore: dataStore}

	path1, checksum1, err := a.buildSSTArchive(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(path1) })

	path2, checksum2, err := a.buildSSTArchive(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(path2) })

	require.Equal(t, checksum1, checksum2,
		"buildSSTArchive must be deterministic — adding a non-deterministic field to periodMetadata is a regression")

	b1, err := os.ReadFile(path1)
	require.NoError(t, err)
	b2, err := os.ReadFile(path2)
	require.NoError(t, err)
	require.True(t, bytes.Equal(b1, b2),
		"SST bytes must be identical across builds of the same period")
}

func TestArchiver_PeriodMetadataHasNoTimestamp(t *testing.T) {
	t.Parallel()

	// Structural check: any future field added to periodMetadata must be a
	// deterministic function of the period. This test fails on a name match
	// against common non-deterministic names — the byte-equality assertion
	// in TestArchiver_BuildSSTIsDeterministic is the load-bearing one, but
	// this gives a clearer error message when someone adds a timestamp.
	typ := reflect.TypeFor[periodMetadata]()

	disallowed := map[string]struct{}{
		"ArchivedAt": {},
		"CreatedAt":  {},
		"Timestamp":  {},
		"Now":        {},
	}

	for f := range typ.Fields() {
		_, bad := disallowed[f.Name]
		require.False(t, bad,
			"field %q is non-deterministic — periodMetadata must stay a pure function of the period (see PR #229)", f.Name)
	}
}
