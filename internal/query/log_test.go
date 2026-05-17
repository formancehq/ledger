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

	"github.com/formancehq/ledger-v3-poc/internal/infra/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
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

	// Test with no logs - should return 0
	lastSequence, err := query.ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastSequence)

	// Insert logs and verify last sequence
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	lastSequence, err = query.ReadLastSequence(s)
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
	lastSequence, err := query.ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence)
}

func TestReadLogsSince(t *testing.T) {
	t.Parallel()

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		cursor, err := query.ReadLogsSince(context.Background(), s, 0)
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

		// afterSequence=0 should return all logs
		cursor, err := query.ReadLogsSince(context.Background(), s, 0)
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

		// afterSequence=2 should return logs 3 and 4
		cursor, err := query.ReadLogsSince(context.Background(), s, 2)
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

		// afterSequence=4 (last log) should return empty
		cursor, err := query.ReadLogsSince(context.Background(), s, 4)
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

		cursor, err := query.ReadLogsSince(context.Background(), s, 999)
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

		// Simulate emitter: read all, then read after cursor
		cursor, err := query.ReadLogsSince(context.Background(), s, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)

		lastSeq := logs[len(logs)-1].GetSequence()

		// Append more logs
		moreLogs := createTestLogsForLedger("test-ledger", 5)
		appendLogs(t, s, 2, moreLogs...)

		// Read only new logs
		cursor, err = query.ReadLogsSince(context.Background(), s, lastSeq)
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

		// Create logs with different payload types
		mixedLogs := []*commonpb.Log{
			{
				Sequence: 1,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreateLedgerLog{
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
						DeleteLedger: &commonpb.DeleteLedgerLog{
							Name:      "new-ledger",
							DeletedAt: commonpb.NewTimestamp(now),
						},
					},
				},
			},
		}
		appendLogs(t, s, 1, mixedLogs...)

		cursor, err := query.ReadLogsSince(context.Background(), s, 0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 3)

		// Verify payload types are preserved
		require.NotNil(t, logs[0].GetPayload().GetCreateLedger())
		require.NotNil(t, logs[1].GetPayload().GetApply())
		require.NotNil(t, logs[2].GetPayload().GetDeleteLedger())
	})
}

// testColdStorage is a test-only in-memory ColdStorage.
type testColdStorage struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newTestColdStorage() *testColdStorage {
	return &testColdStorage{data: make(map[string][]byte)}
}

func (m *testColdStorage) Archive(_ context.Context, bucketID string, periodID uint64, data io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	m.data[fmt.Sprintf("%s/%d", bucketID, periodID)] = buf

	return nil
}

func (m *testColdStorage) Exists(_ context.Context, bucketID string, periodID uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.data[fmt.Sprintf("%s/%d", bucketID, periodID)]

	return ok, nil
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

	// Hot storage has the log → should return it without touching cold
	log, err := query.ReadLogBySequenceWithCold(context.Background(), s, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(1), log.GetSequence())
}

func TestReadLogBySequenceWithCold_NilColdReader(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Not in hot, no cold reader → nil
	log, err := query.ReadLogBySequenceWithCold(context.Background(), s, nil, 999)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLogBySequenceWithCold_ColdFallback(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

	// Create a log to put in cold storage
	coldLog := &commonpb.Log{
		Sequence: 5,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Name: "cold-ledger", CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	}

	// Build SST with the cold log and store in mock cold storage
	cs := newTestColdStorage()
	sstData := buildColdSST(t, coldLog)
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData)))

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
	log, err := query.ReadLogBySequenceWithCold(ctx, s, coldReader, 5)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(5), log.GetSequence())
}

func TestReadLogBySequenceWithCold_NotInCold(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

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
			CreateLedger: &commonpb.CreateLedgerLog{
				Name: "other", CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData)))

	coldReader := coldstorage.NewColdReader(cs, "bucket", t.TempDir(), 4, 0, logger)
	t.Cleanup(func() { _ = coldReader.Close() })

	// Log 5 is NOT in hot storage and NOT in the cold SST
	log, err := query.ReadLogBySequenceWithCold(ctx, s, coldReader, 5)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLogBySequenceWithCold_NoPeriodMatch(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

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
	log, err := query.ReadLogBySequenceWithCold(ctx, s, coldReader, 5)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLogBySequenceWithCold_NonArchivedPeriodIgnored(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := newTestStore(t)

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
	log, err := query.ReadLogBySequenceWithCold(ctx, s, coldReader, 5)
	require.NoError(t, err)
	require.Nil(t, log)
}
