package query_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestReadLogBySequence(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "test-ledger")

	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	log, err := query.ReadLogBySequence(context.Background(), s, 1)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(1), log.GetSequence())

	log, err = query.ReadLogBySequence(context.Background(), s, 999)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLastSequence(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "test-ledger")

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Test with no logs - should return 0
	lastSequence, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastSequence)

	// Insert logs and verify last sequence
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	lastSequence, err = query.ReadLastSequence(handle)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence) // Last log has sequence 4
}

func TestReadLastSequenceAfterSnapshot(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Create some data
	registerLedger(t, s, "test-ledger")

	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	// Create snapshot
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), checkpointID)

	// Verify data still accessible after snapshot
	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	lastSequence, err := query.ReadLastSequence(handle)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence)
}

func TestReadLogsSince(t *testing.T) {
	t.Parallel()

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		cursor, err := query.ReadLogsSince(context.Background(), handle, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("AllLogs", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")

		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		// afterSequence=0 should return all logs
		cursor, err := query.ReadLogsSince(context.Background(), handle, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)
		require.Equal(t, uint64(1), logs[0].GetSequence())
		require.Equal(t, uint64(4), logs[3].GetSequence())
	})

	t.Run("LogsAfterSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")

		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		// afterSequence=2 should return logs 3 and 4
		cursor, err := query.ReadLogsSince(context.Background(), handle, 2)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 2)
		require.Equal(t, uint64(3), logs[0].GetSequence())
		require.Equal(t, uint64(4), logs[1].GetSequence())
	})

	t.Run("LogsAfterLastSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")

		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		// afterSequence=4 (last log) should return empty
		cursor, err := query.ReadLogsSince(context.Background(), handle, 4)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("LogsAfterFarFutureSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")

		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		cursor, err := query.ReadLogsSince(context.Background(), handle, 999)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("IncrementalRead", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")

		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		// Simulate emitter: read all, then read after cursor
		cursor, err := query.ReadLogsSince(context.Background(), handle, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)

		lastSeq := logs[len(logs)-1].GetSequence()

		// Append more logs
		moreLogs := createTestLogsForLedger("test-ledger", 5)
		appendLogs(t, s, 2, moreLogs...)

		// Read only new logs
		cursor, err = query.ReadLogsSince(context.Background(), handle, lastSeq)
		require.NoError(t, err)
		newLogs := collectLogs(t, cursor)
		require.Len(t, newLogs, 4) // 4 new logs starting from sequence 5
		require.Equal(t, uint64(5), newLogs[0].GetSequence())
	})

	t.Run("LogPayloadTypes", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		now := libtime.Now()

		registerLedger(t, s, "test-ledger")

		handle, err := s.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		// Create logs with different payload types
		mixedLogs := []*commonpb.Log{
			{
				Sequence: 1,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreatedLedgerLog{
							Name:      "new-ledger",
							CreatedAt: commonpb.NewTimestamp(now),
						},
					},
				},
			},
			{
				Sequence: 2,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "test-ledger",
							Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &commonpb.CreatedTransaction{
										Transaction: commonpb.NewTransaction().
											WithPostings(
												commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
											).
											WithID(1).
											WithTimestamp(now),
									},
								},
							}).WithID(1).WithDate(now),
						},
					},
				},
			},
			{
				Sequence: 3,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_DeleteLedger{
						DeleteLedger: &commonpb.DeletedLedgerLog{
							Name:      "new-ledger",
							DeletedAt: commonpb.NewTimestamp(now),
						},
					},
				},
			},
		}
		appendLogs(t, s, 1, mixedLogs...)

		cursor, err := query.ReadLogsSince(context.Background(), handle, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 3)

		// Verify payload types are preserved
		require.NotNil(t, logs[0].GetPayload().GetCreateLedger())
		require.NotNil(t, logs[1].GetPayload().GetApply())
		require.NotNil(t, logs[2].GetPayload().GetDeleteLedger())
	})
}

// sha256OrPanic mirrors the small test helper in package coldstorage —
// duplicated here because Go does not share *_test.go helpers across
// packages.
func sha256OrPanic(b []byte) []byte {
	c, err := coldstorage.ComputeSHA256(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}

	return c
}

// testColdStorage is a test-only in-memory ColdStorage.
type testColdStorage struct {
	mu        sync.Mutex
	data      map[string][]byte
	checksums map[string][]byte
}

func newTestColdStorage() *testColdStorage {
	return &testColdStorage{
		data:      make(map[string][]byte),
		checksums: make(map[string][]byte),
	}
}

func (m *testColdStorage) Archive(_ context.Context, bucketID string, periodID uint64, data io.Reader, sha256 []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%d", bucketID, periodID)
	m.data[key] = buf
	m.checksums[key] = append([]byte(nil), sha256...)

	return nil
}

func (m *testColdStorage) Exists(_ context.Context, bucketID string, periodID uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s/%d", bucketID, periodID)
	_, hasData := m.data[key]
	_, hasChecksum := m.checksums[key]

	return hasData && hasChecksum, nil
}

func (m *testColdStorage) ExpectedChecksum(_ context.Context, bucketID string, periodID uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.checksums[fmt.Sprintf("%s/%d", bucketID, periodID)]
	if !ok {
		return nil, coldstorage.ErrChecksumNotFound
	}

	return append([]byte(nil), c...), nil
}

func (m *testColdStorage) Checksum(_ context.Context, bucketID string, periodID uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s/%d", bucketID, periodID)

	buf, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("archive %s not found", key)
	}

	return coldstorage.ComputeSHA256(bytes.NewReader(buf))
}

func (m *testColdStorage) Fetch(_ context.Context, bucketID string, periodID uint64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s/%d", bucketID, periodID)

	buf, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("archive %s not found", key)
	}

	return io.NopCloser(bytes.NewReader(buf)), nil
}

// bufWritable adapts a bytes.Buffer to objstorage.Writable for SST test construction.
type bufWritable struct {
	buf *bytes.Buffer
}

func (w *bufWritable) Write(p []byte) error {
	_, err := w.buf.Write(p)

	return err
}

func (w *bufWritable) Finish() error { return nil }
func (w *bufWritable) Abort()        {}

// buildColdSST builds an SST containing the given log, keyed as Pebble would key it.
func buildColdSST(t *testing.T, logs ...*commonpb.Log) []byte {
	t.Helper()

	var buf bytes.Buffer

	writer := sstable.NewWriter(&bufWritable{buf: &buf}, sstable.WriterOptions{
		Compression:  sstable.SnappyCompression,
		FilterPolicy: bloom.FilterPolicy(10),
	})

	for _, log := range logs {
		kb := dal.NewKeyBuilder()
		kb.PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(log.GetSequence())

		value, err := proto.Marshal(log)
		require.NoError(t, err)
		require.NoError(t, writer.Set(kb.Build(), value))
	}

	require.NoError(t, writer.Close())

	return buf.Bytes()
}

func storePeriod(t *testing.T, s *dal.Store, period *commonpb.Period) {
	t.Helper()

	batch := s.NewBatch()
	require.NoError(t, state.StorePeriod(batch, period))
	require.NoError(t, batch.Commit())
}

func TestReadLogBySequenceWithCold_HotHit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	registerLedger(t, s, "test-ledger")

	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Hot storage has the log → should return it without touching cold
	log, err := query.ReadLogBySequenceWithCold(context.Background(), handle, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(1), log.GetSequence())
}

func TestReadLogBySequenceWithCold_NilColdReader(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Not in hot, no cold reader → nil
	log, err := query.ReadLogBySequenceWithCold(context.Background(), handle, nil, 999)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLogBySequenceWithCold_ColdFallback(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Create a log to put in cold storage
	coldLog := &commonpb.Log{
		Sequence: 5,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name: "cold-ledger", CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	}

	// Build SST with the cold log and store in mock cold storage
	cs := newTestColdStorage()
	sstData := buildColdSST(t, coldLog)
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), sha256OrPanic(sstData)))

	// Store an archived period in hot storage that covers sequence 5
	storePeriod(t, s, &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_ARCHIVED,
		StartSequence: 1,
		CloseSequence: 10,
	})

	coldReader := coldstorage.NewColdReader(cs, "bucket", t.TempDir(), 4, 0, logger)
	t.Cleanup(func() { _ = coldReader.Close() })

	// Log is NOT in hot storage, but IS in cold storage
	log, err := query.ReadLogBySequenceWithCold(ctx, handle, coldReader, 5)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(5), log.GetSequence())
}

func TestReadLogBySequenceWithCold_NotInCold(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Store an archived period, but the cold storage has no data for it
	storePeriod(t, s, &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_ARCHIVED,
		StartSequence: 1,
		CloseSequence: 10,
	})

	cs := newTestColdStorage()
	// Build SST that does NOT contain sequence 5
	sstData := buildColdSST(t, &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name: "other", CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), sha256OrPanic(sstData)))

	coldReader := coldstorage.NewColdReader(cs, "bucket", t.TempDir(), 4, 0, logger)
	t.Cleanup(func() { _ = coldReader.Close() })

	// Log 5 is NOT in hot storage and NOT in the cold SST
	log, err := query.ReadLogBySequenceWithCold(ctx, handle, coldReader, 5)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLogBySequenceWithCold_NoPeriodMatch(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Store a period that does NOT cover the requested sequence
	storePeriod(t, s, &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_ARCHIVED,
		StartSequence: 100,
		CloseSequence: 200,
	})

	cs := newTestColdStorage()
	coldReader := coldstorage.NewColdReader(cs, "bucket", t.TempDir(), 4, 0, logger)
	t.Cleanup(func() { _ = coldReader.Close() })

	// Sequence 5 is not covered by any archived period → nil
	log, err := query.ReadLogBySequenceWithCold(ctx, handle, coldReader, 5)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLogBySequenceWithCold_NonArchivedPeriodIgnored(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Period covers the sequence but is CLOSED (not ARCHIVED) → should not attempt cold read
	storePeriod(t, s, &commonpb.Period{
		Id:            1,
		Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
		StartSequence: 1,
		CloseSequence: 10,
	})

	cs := newTestColdStorage()
	coldReader := coldstorage.NewColdReader(cs, "bucket", t.TempDir(), 4, 0, logger)
	t.Cleanup(func() { _ = coldReader.Close() })

	// Not in hot, period not archived → nil (no cold fetch attempted)
	log, err := query.ReadLogBySequenceWithCold(ctx, handle, coldReader, 5)
	require.NoError(t, err)
	require.Nil(t, log)
}
