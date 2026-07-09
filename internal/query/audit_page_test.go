package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newAuditStore(t *testing.T, seqs ...uint64) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	batch := s.OpenWriteSession()
	for _, seq := range seqs {
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(seq).Build()
		require.NoError(t, batch.SetProto(key, &auditpb.AuditEntry{Sequence: seq}))
	}
	require.NoError(t, batch.Commit())

	return s
}

func collectSeqs(t *testing.T, c cursor.Cursor[*auditpb.AuditEntry]) []uint64 {
	t.Helper()

	entries, err := cursor.Collect(c)
	require.NoError(t, err)

	out := make([]uint64, len(entries))
	for i, e := range entries {
		out[i] = e.GetSequence()
	}

	return out
}

func TestReadAuditEntriesPage_ZoneScanAscending(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, nil, false, 0, ^uint64(0), 0, false, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, collectSeqs(t, c))
}

func TestReadAuditEntriesPage_ZoneScanReverse(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, nil, false, 0, ^uint64(0), 0, true, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{5, 4, 3}, collectSeqs(t, c))
}

func TestReadAuditEntriesPage_ZoneScanCursorAscending(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// afterSeq=2 exclusive, ascending.
	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, nil, false, 0, ^uint64(0), 2, false, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4, 5}, collectSeqs(t, c))
}

func TestReadAuditEntriesPage_ZoneScanCursorReverse(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// afterSeq=4 exclusive, reverse (newest first) -> 3,2,1.
	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, nil, false, 0, ^uint64(0), 4, true, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 2, 1}, collectSeqs(t, c))
}

func TestReadAuditEntriesPage_ZoneScanSeqBounds(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// loSeq=2, hiSeq=4, ascending.
	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, nil, false, 2, 4, 0, false, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3, 4}, collectSeqs(t, c))
}

func TestReadAuditEntriesPage_SeqSetAscending(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Index narrowed to {2, 4, 5}; page 2 ascending.
	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, []uint64{2, 4, 5}, true, 0, ^uint64(0), 0, false, 2)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 4}, collectSeqs(t, c))
}

func TestReadAuditEntriesPage_SeqSetReverseCursor(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Index narrowed {1,2,4,5}, reverse newest-first, afterSeq=4 exclusive -> 2,1.
	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, []uint64{1, 2, 4, 5}, true, 0, ^uint64(0), 4, true, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 1}, collectSeqs(t, c))
}

func TestReadAuditEntriesPage_SeqSetSkipsPurged(t *testing.T) {
	t.Parallel()

	// Zone only has 1 and 5; index references a purged seq 3.
	s := newAuditStore(t, 1, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, []uint64{1, 3, 5}, true, 0, ^uint64(0), 0, false, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 5}, collectSeqs(t, c), "purged seq 3 is skipped")
}

func TestReadAuditEntriesPage_SeqSetWindowFilter(t *testing.T) {
	t.Parallel()

	s := newAuditStore(t, 1, 2, 3, 4, 5)
	handle, err := s.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Index {1,2,3,4,5} but window [2,4].
	c, err := query.ReadAuditEntriesPage(logging.TestingContext(), handle, []uint64{1, 2, 3, 4, 5}, true, 2, 4, 0, false, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3, 4}, collectSeqs(t, c))
}
