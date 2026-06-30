package check

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestCompareIdempotencyOutcomes_ArchivedFreezeWithinTTLWindow covers the
// completeness gap closed by reDeriveArchivedIdempotency: a frozen idempotency
// entry whose freezing audit entry has been ARCHIVED but is still within the
// idempotency TTL window is re-derived from cold storage and verified, while an
// entry frozen by an archived audit entry OLDER than the window is skipped (no
// false positive). Bounding is asserted by leaving a fully-below-window archived
// chapter's SST absent: if the pass read it, the cold read would fail, the floor
// would fall back to the archive boundary, and the in-window tamper would no
// longer be flagged.
func TestCompareIdempotencyOutcomes_ArchivedFreezeWithinTTLWindow(t *testing.T) {
	t.Parallel()

	const (
		clusterID = "idem-cold-cluster"
		bucketID  = "test-bucket"

		liveKey    = "live-key"    // frozen by an archived entry inside the TTL window
		expiredKey = "expired-key" // frozen by an archived entry older than the window

		ttlCutoff = 2000 // lower bound of the TTL window
		tsExpired = 1500 // archived freeze below the window
		tsLive    = 3000 // archived freeze inside the window
		tsHot     = 5000 // surviving post-archive entry => verifiedRangeStartTs

		chapterNewer = 7 // archived chapter holding audit seq 3-4 (straddles the cutoff)
		chapterOlder = 3 // archived chapter holding audit seq 1-2 (entirely below the window)
	)

	store := createTestStore(t)

	// One real order shared by every freeze; its serialized form drives the
	// proposal hash both in the audit items and in the stored projection.
	orders := []*raftcmdpb.Order{{}}
	serialized := orders[0].MarshalDeterministicVT(nil)
	proposalHash := processing.HashOrders(orders)

	failure := func(message string, ctx map[string]string) *auditpb.AuditEntry_Failure {
		return &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{
			Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
			Message: message,
			Context: ctx,
		}}
	}

	// Archived audit entries, both living in the newer chapter's SST: seq 3 is
	// below the cutoff (skipped), seq 4 is inside the window (re-derived).
	coldEntries := []*auditpb.AuditEntry{
		{
			Sequence: 3, Timestamp: &commonpb.Timestamp{Data: tsExpired}, ProposalId: 1, OrderCount: 1,
			HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
			Idempotency: &commonpb.Idempotency{Key: expiredKey},
			Outcome:     failure("old", map[string]string{"a": "1"}),
		},
		{
			Sequence: 4, Timestamp: &commonpb.Timestamp{Data: tsLive}, ProposalId: 2, OrderCount: 1,
			HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
			Idempotency: &commonpb.Idempotency{Key: liveKey},
			Outcome:     failure("balance too low", map[string]string{"account": "bank"}),
		},
	}
	coldItems := map[uint64][]*auditpb.AuditItem{
		3: {{OrderIndex: 0, SerializedOrder: serialized}},
		4: {{OrderIndex: 0, SerializedOrder: serialized}},
	}

	// Upload only the newer chapter's archive; the older chapter is deliberately
	// missing so any attempt to read it surfaces as a cold-read failure.
	fs := coldstorage.NewFilesystemStorage(t.TempDir())
	sstBytes := buildColdAuditSST(t, coldEntries, coldItems)

	checksum, err := coldstorage.ComputeSHA256(bytes.NewReader(sstBytes))
	require.NoError(t, err)
	require.NoError(t, fs.Archive(context.Background(), bucketID, chapterNewer, bytes.NewReader(sstBytes), checksum))

	coldReader := coldstorage.NewColdReader(fs, bucketID, t.TempDir(), 4, 0, logging.Testing())
	t.Cleanup(func() { _ = coldReader.Close() })

	// A surviving post-archive entry anchors the verified range (seq after both
	// chapters' close audit sequence). archiveLastAuditHash is nil, so its hash
	// chains from a nil seed — matching persistAuditEntry.
	hot := &auditpb.AuditEntry{
		Sequence: 5, Timestamp: &commonpb.Timestamp{Data: tsHot}, ProposalId: 9,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Outcome:     failure("unrelated", nil),
	}
	persistAuditEntry(t, store, hot, nil, clusterID)

	chapters := []*commonpb.Chapter{
		{Id: chapterNewer, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED, StartAuditSequence: 3, CloseAuditSequence: 4},
		{Id: chapterOlder, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED, StartAuditSequence: 1, CloseAuditSequence: 2},
	}

	collectMismatches := func() []*servicepb.CheckStoreError {
		checker := NewChecker(store, attributes.New(), clusterID, coldReader, logging.Testing())

		handle, err := store.NewReadHandle()
		require.NoError(t, err)

		defer func() { _ = handle.Close() }()

		var got []*servicepb.CheckStoreError

		require.NoError(t, checker.verifyAuditHashChain(context.Background(), handle, chapters, nil, ttlCutoff,
			func(event *servicepb.CheckStoreEvent) {
				if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
					e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH {
					got = append(got, e.Error)
				}
			}))

		return got
	}

	// expiredKey's freeze (created_at=1500) is older than the window — it has no
	// re-derived expectation and must be skipped, not flagged.
	writeIdempotencyEntry(t, store, expiredKey, &commonpb.IdempotencyKeyValue{
		CreatedAt: tsExpired,
		Hash:      proposalHash,
		Failure:   &commonpb.IdempotencyFailure{Reason: commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS, Message: "old", Metadata: map[string]string{"a": "1"}},
	})

	faithfulLive := &commonpb.IdempotencyKeyValue{
		CreatedAt: tsLive,
		Hash:      proposalHash,
		Failure:   &commonpb.IdempotencyFailure{Reason: commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS, Message: "balance too low", Metadata: map[string]string{"account": "bank"}},
	}
	writeIdempotencyEntry(t, store, liveKey, faithfulLive)

	require.Empty(t, collectMismatches(),
		"a faithful entry frozen by an archived-but-in-window audit entry must pass, and an older-than-window entry must be skipped")

	tampered := faithfulLive.CloneVT()
	tampered.Failure.Message = "you have plenty of money"
	writeIdempotencyEntry(t, store, liveKey, tampered)

	got := collectMismatches()
	require.NotEmpty(t, got,
		"a tampered outcome on a live entry whose freeze is archived within the TTL window must be flagged")
	require.Contains(t, got[0].GetMessage(), "3000",
		"the flagged mismatch should reference the in-window created_at")
}

// buildColdAuditSST builds the SST an archived chapter would hold for the given
// audit entries and items, keyed exactly as the live store keys them so the
// query helpers read them back unchanged from a ColdReader.
func buildColdAuditSST(t *testing.T, entries []*auditpb.AuditEntry, items map[uint64][]*auditpb.AuditItem) []byte {
	t.Helper()

	type kv struct{ k, v []byte }

	var kvs []kv

	for _, e := range entries {
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(e.GetSequence()).Build()

		val, err := e.MarshalVT()
		require.NoError(t, err)

		kvs = append(kvs, kv{key, val})
	}

	for seq, list := range items {
		for _, it := range list {
			key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(seq).PutUint32(it.GetOrderIndex()).Build()

			val, err := it.MarshalVT()
			require.NoError(t, err)

			kvs = append(kvs, kv{key, val})
		}
	}

	sort.Slice(kvs, func(i, j int) bool { return bytes.Compare(kvs[i].k, kvs[j].k) < 0 })

	var buf bytes.Buffer

	w := sstable.NewWriter(newSSTBufWritable(&buf), sstable.WriterOptions{
		Compression:  sstable.SnappyCompression,
		FilterPolicy: bloom.FilterPolicy(10),
	})

	for _, p := range kvs {
		require.NoError(t, w.Set(p.k, p.v))
	}

	require.NoError(t, w.Close())

	return buf.Bytes()
}

// sstBufWritable adapts a bytes.Buffer to the objstorage.Writable interface
// sstable.NewWriter expects.
type sstBufWritable struct {
	buf *bytes.Buffer
}

func newSSTBufWritable(buf *bytes.Buffer) *sstBufWritable {
	return &sstBufWritable{buf: buf}
}

func (w *sstBufWritable) Write(p []byte) error {
	_, err := w.buf.Write(p)

	return err
}

func (w *sstBufWritable) Finish() error { return nil }
func (w *sstBufWritable) Abort()        {}
