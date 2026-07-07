package check

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/infra/state"
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

		ttlCutoff = 2000 // desired lower bound of the TTL window
		tsExpired = 1500 // archived freeze below the window
		tsLive    = 3000 // archived freeze inside the window
		tsHot     = 5000 // surviving post-archive entry => verified "now"

		// verifyAuditHashChain derives the cutoff as (verified now − TTL); with
		// a single hot entry, verified now == tsHot, so this TTL yields ttlCutoff.
		ttlMicros = tsHot - ttlCutoff

		chapterNewer = 7 // archived chapter holding audit seq 3-4 (straddles the cutoff)
		chapterOlder = 3 // archived chapter holding audit seq 1-2 (entirely below the window)
	)

	store := createTestStore(t)

	// One real order shared by every freeze; its serialized form drives the
	// proposal hash both in the audit items and in the stored projection.
	orders := []*raftcmdpb.Order{{}}
	serialized := orders[0].MarshalDeterministicVT(nil)
	proposalHash := processing.HashOrders(orders)

	// Archived audit entries, both living in the newer chapter's SST: seq 3 is
	// below the cutoff (skipped), seq 4 is inside the window (re-derived).
	coldEntries := []*auditpb.AuditEntry{
		{
			Sequence: 3, Timestamp: &commonpb.Timestamp{Data: tsExpired}, ProposalId: 1, OrderCount: 1,
			HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
			Idempotency: &commonpb.Idempotency{Key: expiredKey},
			Outcome:     idemAuditFailure("old", map[string]string{"a": "1"}),
		},
		{
			Sequence: 4, Timestamp: &commonpb.Timestamp{Data: tsLive}, ProposalId: 2, OrderCount: 1,
			HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
			Idempotency: &commonpb.Idempotency{Key: liveKey},
			Outcome:     idemAuditFailure("balance too low", map[string]string{"account": "bank"}),
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
		Outcome:     idemAuditFailure("unrelated", nil),
	}
	persistAuditEntry(t, store, hot, nil, clusterID)

	chapters := []*commonpb.Chapter{
		{Id: chapterNewer, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED, StartAuditSequence: 3, CloseAuditSequence: 4},
		{Id: chapterOlder, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED, StartAuditSequence: 1, CloseAuditSequence: 2},
	}

	collectMismatches := func() []*servicepb.CheckStoreError {
		checker := NewChecker(store, attributes.New(), clusterID, coldReader, nil, logging.Testing())

		handle, err := store.NewReadHandle()
		require.NoError(t, err)

		defer func() { _ = handle.Close() }()

		var got []*servicepb.CheckStoreError

		ttl := uint64(ttlMicros)
		require.NoError(t, checker.verifyAuditHashChain(context.Background(), handle, chapters, nil, &ttl,
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

// TestCompareIdempotencyOutcomes_NeverExpireScansFullArchivedHistory pins the
// idempotency-ttl=0 (never expire) behavior: the cold re-derivation scans the
// full archived history (ttlCutoff 0), so even a freeze far older than any
// finite window is verified, and the report floor of 0 reports every unmatched
// entry. It also pins the documented non-goal: a frozen key with no stored
// entry (a deletion) is not flagged, because eviction is unaudited and a missing
// entry can't be told apart from a legitimately evicted one.
func TestCompareIdempotencyOutcomes_NeverExpireScansFullArchivedHistory(t *testing.T) {
	t.Parallel()

	const (
		clusterID  = "idem-never-expire-cluster"
		bucketID   = "test-bucket"
		ancientKey = "ancient-key"

		tsAncient = 500  // far below any finite TTL window
		tsHot     = 5000 // surviving post-archive entry => verifiedRangeStartTs

		chapterAncient = 11
	)

	store := createTestStore(t)

	orders := []*raftcmdpb.Order{{}}
	serialized := orders[0].MarshalDeterministicVT(nil)
	proposalHash := processing.HashOrders(orders)

	coldEntries := []*auditpb.AuditEntry{
		{
			Sequence: 1, Timestamp: &commonpb.Timestamp{Data: tsAncient}, ProposalId: 1, OrderCount: 1,
			HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
			Idempotency: &commonpb.Idempotency{Key: ancientKey},
			Outcome:     idemAuditFailure("ancient", map[string]string{"k": "v"}),
		},
	}
	coldItems := map[uint64][]*auditpb.AuditItem{1: {{OrderIndex: 0, SerializedOrder: serialized}}}

	fs := coldstorage.NewFilesystemStorage(t.TempDir())
	sstBytes := buildColdAuditSST(t, coldEntries, coldItems)

	checksum, err := coldstorage.ComputeSHA256(bytes.NewReader(sstBytes))
	require.NoError(t, err)
	require.NoError(t, fs.Archive(context.Background(), bucketID, chapterAncient, bytes.NewReader(sstBytes), checksum))

	coldReader := coldstorage.NewColdReader(fs, bucketID, t.TempDir(), 4, 0, logging.Testing())
	t.Cleanup(func() { _ = coldReader.Close() })

	hot := &auditpb.AuditEntry{
		Sequence: 5, Timestamp: &commonpb.Timestamp{Data: tsHot}, ProposalId: 9,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Outcome:     idemAuditFailure("unrelated", nil),
	}
	persistAuditEntry(t, store, hot, nil, clusterID)

	chapters := []*commonpb.Chapter{
		{Id: chapterAncient, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED, StartAuditSequence: 1, CloseAuditSequence: 2},
	}

	collectMismatches := func() []*servicepb.CheckStoreError {
		checker := NewChecker(store, attributes.New(), clusterID, coldReader, nil, logging.Testing())

		handle, err := store.NewReadHandle()
		require.NoError(t, err)

		defer func() { _ = handle.Close() }()

		var got []*servicepb.CheckStoreError

		// TTL 0 is never-expire: the derived cutoff is 0, so the cold pass scans
		// the full archived history.
		neverExpire := uint64(0)
		require.NoError(t, checker.verifyAuditHashChain(context.Background(), handle, chapters, nil, &neverExpire,
			func(event *servicepb.CheckStoreEvent) {
				if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
					e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH {
					got = append(got, e.Error)
				}
			}))

		return got
	}

	// Deletion non-goal: the audit froze ancientKey, but with no stored entry
	// the pass reports nothing — a missing entry is indistinguishable from a
	// legitimately evicted one.
	require.Empty(t, collectMismatches(),
		"a frozen key with no stored entry must not be flagged (deletion is out of scope)")

	faithful := &commonpb.IdempotencyKeyValue{
		CreatedAt: tsAncient,
		Hash:      proposalHash,
		Failure:   &commonpb.IdempotencyFailure{Reason: commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS, Message: "ancient", Metadata: map[string]string{"k": "v"}},
	}
	writeIdempotencyEntry(t, store, ancientKey, faithful)

	require.Empty(t, collectMismatches(),
		"never-expire must scan the full archived history and pass a faithful ancient entry")

	tampered := faithful.CloneVT()
	tampered.Failure.Message = "tampered"
	writeIdempotencyEntry(t, store, ancientKey, tampered)

	got := collectMismatches()
	require.NotEmpty(t, got,
		"never-expire must scan the full archived history and flag a tampered ancient entry")
	require.Contains(t, got[0].GetMessage(), "500",
		"the flagged mismatch should reference the ancient created_at")
}

// TestReDeriveArchivedIdempotency_Bounds exercises the branch behaviour of the
// cold re-derivation directly: coverage of archived data, the no-cold-reader and
// failed-read fallbacks, the newest-first scan stopping at the first chapter
// entirely below the window, and skipping non-keyed entries.
func TestReDeriveArchivedIdempotency_Bounds(t *testing.T) {
	t.Parallel()

	const bucketID = "test-bucket"

	store := createTestStore(t)
	ctx := context.Background()

	archived := func(id, closeAuditSeq uint64) *commonpb.Chapter {
		return &commonpb.Chapter{Id: id, Status: commonpb.ChapterStatus_CHAPTER_ARCHIVED, CloseAuditSequence: closeAuditSeq}
	}

	t.Run("no archived chapters is fully covered without a cold reader", func(t *testing.T) {
		t.Parallel()

		c := NewChecker(store, attributes.New(), "x", nil, nil, logging.Testing())
		require.True(t, c.reDeriveArchivedIdempotency(ctx, nil, 0, map[idemExpectedKey]expectedIdempotency{}))
	})

	t.Run("archived data with no cold reader is not covered", func(t *testing.T) {
		t.Parallel()

		c := NewChecker(store, attributes.New(), "x", nil, nil, logging.Testing())
		require.False(t, c.reDeriveArchivedIdempotency(ctx, []*commonpb.Chapter{archived(1, 2)}, 0, map[idemExpectedKey]expectedIdempotency{}))
	})

	t.Run("a missing archive is a read failure, not coverage", func(t *testing.T) {
		t.Parallel()

		// Cold reader over an empty store: GetReader fails for the chapter.
		reader := coldReaderWithChapters(t, bucketID, nil)
		c := NewChecker(store, attributes.New(), "x", reader, nil, logging.Testing())
		require.False(t, c.reDeriveArchivedIdempotency(ctx, []*commonpb.Chapter{archived(99, 2)}, 0, map[idemExpectedKey]expectedIdempotency{}))
	})

	t.Run("scan stops at the first chapter below the window and skips non-keyed entries", func(t *testing.T) {
		t.Parallel()

		const cutoff = 2000

		serialized := (&raftcmdpb.Order{}).MarshalDeterministicVT(nil)

		// Newer chapter: one keyed entry in-window + one non-keyed entry.
		newer := buildColdAuditSST(t, []*auditpb.AuditEntry{
			{Sequence: 5, Timestamp: &commonpb.Timestamp{Data: 3000}, Idempotency: &commonpb.Idempotency{Key: "k"}, Outcome: idemAuditFailure("m", nil)},
			{Sequence: 6, Timestamp: &commonpb.Timestamp{Data: 4000}, Outcome: idemAuditFailure("no-key", nil)},
		}, map[uint64][]*auditpb.AuditItem{5: {{OrderIndex: 0, SerializedOrder: serialized}}})

		// Older chapter: entirely below the cutoff — must not be scanned for items.
		older := buildColdAuditSST(t, []*auditpb.AuditEntry{
			{Sequence: 3, Timestamp: &commonpb.Timestamp{Data: 500}, Idempotency: &commonpb.Idempotency{Key: "old"}, Outcome: idemAuditFailure("x", nil)},
			{Sequence: 4, Timestamp: &commonpb.Timestamp{Data: 1000}, Idempotency: &commonpb.Idempotency{Key: "old2"}, Outcome: idemAuditFailure("y", nil)},
		}, map[uint64][]*auditpb.AuditItem{4: {{OrderIndex: 0, SerializedOrder: serialized}}})

		reader := coldReaderWithChapters(t, bucketID, map[uint64][]byte{20: newer, 10: older})
		c := NewChecker(store, attributes.New(), "x", reader, nil, logging.Testing())

		expected := map[idemExpectedKey]expectedIdempotency{}
		require.True(t, c.reDeriveArchivedIdempotency(ctx, []*commonpb.Chapter{archived(20, 6), archived(10, 4)}, cutoff, expected))

		// Only the in-window keyed entry was re-derived: the non-keyed entry was
		// skipped, and the older chapter (below the window) was never scanned.
		require.Len(t, expected, 1)
		_, ok := expected[idemExpectedKey{keyHash: state.HashIdempotencyKey("k"), createdAt: 3000}]
		require.True(t, ok, "the in-window keyed entry must be re-derived")
	})

	t.Run("success outcomes are re-derived from cold storage", func(t *testing.T) {
		t.Parallel()

		serialized := (&raftcmdpb.Order{}).MarshalDeterministicVT(nil)

		sst := buildColdAuditSST(t, []*auditpb.AuditEntry{
			{
				Sequence: 5, Timestamp: &commonpb.Timestamp{Data: 3000}, Idempotency: &commonpb.Idempotency{Key: "s"},
				Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 10, MaxLogSequence: 12}},
			},
		}, map[uint64][]*auditpb.AuditItem{5: {{OrderIndex: 0, SerializedOrder: serialized}}})

		reader := coldReaderWithChapters(t, bucketID, map[uint64][]byte{30: sst})
		c := NewChecker(store, attributes.New(), "x", reader, nil, logging.Testing())

		expected := map[idemExpectedKey]expectedIdempotency{}
		require.True(t, c.reDeriveArchivedIdempotency(ctx, []*commonpb.Chapter{archived(30, 5)}, 2000, expected))

		got, ok := expected[idemExpectedKey{keyHash: state.HashIdempotencyKey("s"), createdAt: 3000}]
		require.True(t, ok, "the success freeze must be re-derived")
		require.False(t, got.failure)
		require.Equal(t, uint64(10), got.firstLog)
		require.Equal(t, uint32(3), got.logCount, "log count is max−min+1")
	})

	t.Run("a chapter with no audit entries is skipped", func(t *testing.T) {
		t.Parallel()

		// SST holds only a (non-audit) log key, so ReadLastAuditEntry returns
		// nil and the chapter is skipped without error.
		logKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(1).Build()
		sst := writeSSTBytes(t, [][2][]byte{{logKey, []byte("x")}})

		reader := coldReaderWithChapters(t, bucketID, map[uint64][]byte{40: sst})
		c := NewChecker(store, attributes.New(), "x", reader, nil, logging.Testing())

		expected := map[idemExpectedKey]expectedIdempotency{}
		require.True(t, c.reDeriveArchivedIdempotency(ctx, []*commonpb.Chapter{archived(40, 2)}, 2000, expected))
		require.Empty(t, expected)
	})
}

// TestCheck_DerivesIdempotencyTTLWindowFromPersistedConfig exercises the
// end-to-end path in Check that reads the persisted idempotency TTL and passes
// it to the hash-chain pass, which derives the window cutoff from the verified
// "now". A short TTL keeps the cutoff bounded (verified now − TTL > 0). A clean
// store must still pass.
func TestCheck_DerivesIdempotencyTTLWindowFromPersistedConfig(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("test"))
	engine.processAndCommit(createTransactionOrder("test", true,
		newPosting("world", "user:alice", "USD", 100)))

	// A 1s TTL is well below the engine's audit timestamps, so the derived
	// cutoff is non-zero (exercises the bounded-window branch). "now" comes from
	// the verified chain, not a persisted last-applied timestamp.
	writePersistedConfig(t, engine.store, &commonpb.PersistedConfig{
		ClusterId:             engine.clusterID,
		IdempotencyTtlSeconds: 1,
	})

	require.Empty(t, collectCheckErrors(t, engine.store, engine.attrs),
		"a clean store with a persisted idempotency TTL must pass Check")
}

// TestResolveIdempotencyTTLMicros pins the TTL-source precedence: the trusted
// runtime config wins over the persisted projection, the persisted value is the
// fallback, an explicit never-expire (0) runtime TTL is preserved (not confused
// with "unset"), and nil is returned when neither source exists.
func TestResolveIdempotencyTTLMicros(t *testing.T) {
	t.Parallel()

	dur := func(d time.Duration) *time.Duration { return &d }

	t.Run("runtime config wins over persisted", func(t *testing.T) {
		t.Parallel()

		got := resolveIdempotencyTTLMicros(dur(2*time.Second), &commonpb.PersistedConfig{IdempotencyTtlSeconds: 99})
		require.NotNil(t, got)
		require.Equal(t, uint64(2_000_000), *got)
	})

	t.Run("runtime never-expire (0) wins and is preserved", func(t *testing.T) {
		t.Parallel()

		got := resolveIdempotencyTTLMicros(dur(0), &commonpb.PersistedConfig{IdempotencyTtlSeconds: 99})
		require.NotNil(t, got)
		require.Equal(t, uint64(0), *got, "an explicit runtime TTL of 0 must not fall back to persisted")
	})

	t.Run("falls back to persisted when no runtime config", func(t *testing.T) {
		t.Parallel()

		got := resolveIdempotencyTTLMicros(nil, &commonpb.PersistedConfig{IdempotencyTtlSeconds: 3})
		require.NotNil(t, got)
		require.Equal(t, uint64(3_000_000), *got)
	})

	t.Run("nil when neither is available", func(t *testing.T) {
		t.Parallel()

		require.Nil(t, resolveIdempotencyTTLMicros(nil, nil))
	})
}

// writePersistedConfig stores the PersistedConfig at its Global-zone key, the
// layout query.ReadPersistedConfig reads.
func writePersistedConfig(t *testing.T, store *dal.Store, cfg *commonpb.PersistedConfig) {
	t.Helper()

	data, err := cfg.MarshalVT()
	require.NoError(t, err)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}, data))
	require.NoError(t, batch.Commit())
}

// coldReaderWithChapters builds a ColdReader backed by a filesystem store
// holding the given chapter SSTs (chapterID -> SST bytes).
func coldReaderWithChapters(t *testing.T, bucketID string, sstByChapter map[uint64][]byte) *coldstorage.ColdReader {
	t.Helper()

	fs := coldstorage.NewFilesystemStorage(t.TempDir())

	for id, sst := range sstByChapter {
		checksum, err := coldstorage.ComputeSHA256(bytes.NewReader(sst))
		require.NoError(t, err)
		require.NoError(t, fs.Archive(context.Background(), bucketID, id, bytes.NewReader(sst), checksum))
	}

	reader := coldstorage.NewColdReader(fs, bucketID, t.TempDir(), 8, 0, logging.Testing())
	t.Cleanup(func() { _ = reader.Close() })

	return reader
}

// idemAuditFailure builds a freezable-failure outcome for an archived audit
// entry under test.
func idemAuditFailure(message string, ctx map[string]string) *auditpb.AuditEntry_Failure {
	return &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{
		Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
		Message: message,
		Context: ctx,
	}}
}

// buildColdAuditSST builds the SST an archived chapter would hold for the given
// audit entries and items, keyed exactly as the live store keys them so the
// query helpers read them back unchanged from a ColdReader.
func buildColdAuditSST(t *testing.T, entries []*auditpb.AuditEntry, items map[uint64][]*auditpb.AuditItem) []byte {
	t.Helper()

	var kvs [][2][]byte

	for _, e := range entries {
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(e.GetSequence()).Build()

		val, err := e.MarshalVT()
		require.NoError(t, err)

		kvs = append(kvs, [2][]byte{key, val})
	}

	for seq, list := range items {
		for _, it := range list {
			key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(seq).PutUint32(it.GetOrderIndex()).Build()

			val, err := it.MarshalVT()
			require.NoError(t, err)

			kvs = append(kvs, [2][]byte{key, val})
		}
	}

	return writeSSTBytes(t, kvs)
}

// writeSSTBytes writes an SST (sorted by key) with the given key/value pairs and
// returns its bytes.
func writeSSTBytes(t *testing.T, kvs [][2][]byte) []byte {
	t.Helper()

	sort.Slice(kvs, func(i, j int) bool { return bytes.Compare(kvs[i][0], kvs[j][0]) < 0 })

	var buf bytes.Buffer

	w := sstable.NewWriter(newSSTBufWritable(&buf), sstable.WriterOptions{
		Compression:  sstable.SnappyCompression,
		FilterPolicy: bloom.FilterPolicy(10),
	})

	for _, kv := range kvs {
		require.NoError(t, w.Set(kv[0], kv[1]))
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
