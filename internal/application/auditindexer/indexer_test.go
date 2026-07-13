package auditindexer

import (
	"context"
	"testing"
	"time"

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

// dumpAuditIndexKeys returns every audit-index key currently stored, as a
// sorted slice, for byte-identical parity comparisons.
func dumpAuditIndexKeys(t *testing.T, rs *readstore.Store) [][]byte {
	t.Helper()

	return rs.DumpAuditIndexKeysForTest()
}

func TestRebuildYieldsIdenticalIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	idx, mainStore, rs := newIndexerForTest(t)

	for s := uint64(1); s <= 5; s++ {
		writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
			Sequence: s, ProposalId: s, Timestamp: &commonpb.Timestamp{Data: s * 1_000_000},
			Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
			Ledgers: []string{"main"},
		})
	}

	_, err := idx.ProcessOnce(ctx)
	require.NoError(t, err)
	before := dumpAuditIndexKeys(t, rs)
	require.NotEmpty(t, before)

	require.NoError(t, idx.Rebuild(ctx))
	after := dumpAuditIndexKeys(t, rs)

	require.Equal(t, before, after, "rebuild must yield a byte-identical index")

	cursor, err := rs.ReadAuditProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(5), cursor)
}

// TestShouldRebuildOnBoot covers the sole retained rebuild trigger: a missing
// cursor (0) with entries present (fresh index over a populated audit zone).
// The audit chain is append-only and monotone, so a "cursor ahead of head"
// state is unreachable and no gap-based heuristic exists — a lagging cursor is
// resumed incrementally, never rebuilt from 0.
func TestShouldRebuildOnBoot(t *testing.T) {
	t.Parallel()
	idx, _, _ := newIndexerForTest(t)

	require.True(t, idx.shouldRebuildOnBoot(0, 5), "missing cursor with entries -> rebuild")
	require.False(t, idx.shouldRebuildOnBoot(0, 0), "empty store -> no rebuild")
	require.False(t, idx.shouldRebuildOnBoot(5, 10), "cursor lagging head -> incremental catch-up, no rebuild")
	require.False(t, idx.shouldRebuildOnBoot(5, 1_000_000), "large lag -> incremental catch-up, no rebuild")
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

	processed, err = idx2.ProcessOnce(ctx2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), processed)

	seqs, err = rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, seqs)
}

// TestProcessOnceWakesAuditWaiters verifies the indexer wakes readers blocked in
// WaitForAuditSequence as soon as it commits the audit cursor, rather than
// leaving them parked until an unrelated log-index notification or the next tick.
// This is the wakeup the filtered-audit read-your-writes gate depends on.
func TestProcessOnceWakesAuditWaiters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	idx, mainStore, rs := newIndexerForTest(t)

	// A waiter for an audit entry that does not exist yet must block.
	waitErr := make(chan error, 1)
	go func() {
		wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waitErr <- rs.WaitForAuditSequence(wctx, 1)
	}()

	select {
	case err := <-waitErr:
		t.Fatalf("WaitForAuditSequence returned before the entry was indexed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Write the audit entry and index it. processBatch commits the cursor then
	// calls NotifyProgress, which must wake the waiter.
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

	select {
	case err := <-waitErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForAuditSequence did not wake after ProcessOnce committed the cursor")
	}
}

func TestStartStopIndexes(t *testing.T) {
	t.Parallel()
	idx, mainStore, rs := newIndexerForTest(t)

	writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
		Sequence: 1, ProposalId: 1, Timestamp: &commonpb.Timestamp{Data: 1_000_000},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Ledgers: []string{"main"},
	})

	idx.Start()
	t.Cleanup(idx.Stop)

	require.Eventually(t, func() bool {
		seqs, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")

		return err == nil && len(seqs) == 1
	}, 5*time.Second, 20*time.Millisecond)
}

// TestProcessOnceHonorsContextCancellation asserts the drain loop checks the
// context between batches: with a backlog present and an already-cancelled
// context, ProcessOnce must abort immediately (returning context.Canceled)
// instead of draining to completion, so worker.Stop() cannot hang on a large
// backlog or sustained write stream during shutdown.
func TestProcessOnceHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	idx, mainStore, rs := newIndexerForTest(t)
	idx.batchSize = 1 // a full drain would take several iterations

	for s := uint64(1); s <= 5; s++ {
		writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
			Sequence: s, ProposalId: s, Timestamp: &commonpb.Timestamp{Data: s * 1_000_000},
			Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
			Ledgers: []string{"main"},
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before any draining begins

	cursor, err := idx.ProcessOnce(ctx)
	require.ErrorIs(t, err, context.Canceled, "a cancelled context must abort the drain loop")
	require.Equal(t, uint64(0), cursor, "no batch should be committed after cancellation")

	persisted, err := rs.ReadAuditProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(0), persisted, "cursor must not advance when cancelled")

	seqs, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Empty(t, seqs, "nothing should be indexed when cancelled before draining")
}

func TestIndexerKeepsUpUnderLoad(t *testing.T) {
	t.Parallel()
	idx, mainStore, rs := newIndexerForTest(t)
	idx.Start()
	t.Cleanup(idx.Stop)

	const total = 200
	for s := uint64(1); s <= total; s++ {
		writeAuditEntry(t, mainStore, &auditpb.AuditEntry{
			Sequence: s, ProposalId: s, Timestamp: &commonpb.Timestamp{Data: s * 1_000_000},
			Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
			Ledgers: []string{"main"},
		})
	}

	require.Eventually(t, func() bool {
		c, err := rs.ReadAuditProgress()

		return err == nil && c == total
	}, 10*time.Second, 50*time.Millisecond)

	seqs, err := rs.AuditSeqsByString(readstore.AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Len(t, seqs, total)
}
