package auditindexer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func writeAuditEntry(t *testing.T, store *dal.Store, entry *auditpb.AuditEntry) {
	t.Helper()

	batch := store.OpenWriteSession()
	kb := dal.NewKeyBuilder()

	val, err := proto.Marshal(entry)
	require.NoError(t, err)

	require.NoError(t, batch.SetBytes(
		kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(entry.GetSequence()).Build(),
		val,
	))
	require.NoError(t, batch.Commit())
}

func newIndexerForTest(t *testing.T) (*Indexer, *dal.Store, *readstore.Store) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	mainStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mainStore.Close() })

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	idx := New(Config{}, mainStore, rs, logger, meter)

	return idx, mainStore, rs
}

func TestIndexerCatchUpAndResume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	idx, mainStore, rs := newIndexerForTest(t)

	writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
		Sequence:   1,
		ProposalId: 7,
		Timestamp:  &commonpb.Timestamp{Data: 1_000_000},
		Outcome:    &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Ledgers:    []string{"main"},
	})

	processed, err := idx.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), processed)

	seqs, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, seqs)

	cursor, err := rs.ReadAuditProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(1), cursor)

	// Second ProcessOnce with no new entries — cursor stays the same.
	processed, err = idx.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), processed)

	// New indexer picks up from persisted cursor.
	ctx2 := logging.TestingContext()
	logger2 := logging.FromContext(ctx2)
	meter2 := noop.NewMeterProvider().Meter("test")
	idx2 := New(Config{}, mainStore, rs, logger2, meter2)

	writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
		Sequence:   2,
		ProposalId: 8,
		Timestamp:  &commonpb.Timestamp{Data: 2_000_000},
		Outcome:    &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}},
		Ledgers:    []string{"main"},
	})

	processed, err = idx2.ProcessOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), processed)

	seqs, err = rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, seqs)
}
