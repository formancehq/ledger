package check

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// errorsOfType filters a Check() run down to one error type.
//
// The filter is load-bearing on the fixtures that report many findings at once —
// the archived and forged-boundary shapes, where a clipped replay legitimately
// trips the volume, transaction, boundary, unaudited-ledger and signing passes
// as well. Fixtures whose injected row reaches nothing else assert a total count
// instead, which is the stronger claim; prefer that where it holds.
func errorsOfType(errs []*servicepb.CheckStoreError, errorType servicepb.CheckStoreErrorType) []*servicepb.CheckStoreError {
	var out []*servicepb.CheckStoreError

	for _, e := range errs {
		if e.GetErrorType() == errorType {
			out = append(out, e)
		}
	}

	return out
}

// TestCheckDetectsLogAboveAuditedMaximum pins the log-bounds comparison. Log
// rows are not hash-chain bound and the checker REPLAYS them, so a row appended
// past the last audited proposal is not merely unverified — every projection
// pass folds it into the expectation it then compares the store against. The
// audited maximum is the only value that contradicts such a row.
//
// The injected row carries no payload, so nothing but the bound (and the
// payload-agnostic elision guard) can see it: the transaction, volume, metadata
// and boundary passes all have nothing to disagree with.
func TestCheckDetectsLogAboveAuditedMaximum(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("ledger"))

	healthy := collectCheckErrors(t, engine.store, engine.attrs)
	require.Empty(t, errorsOfType(healthy, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"a store whose highest log is the highest audited log must not be flagged")
	require.Empty(t, errorsOfType(healthy, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
		"a chain walk that reached the end of the live range must compare, not report incomplete")

	// Append a log one sequence above the audited maximum. Key and value agree,
	// so the key/value divergence assertion stays silent and the only thing
	// wrong with the row is that no audit entry accounts for it.
	const unaudited = 2

	batch := engine.store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(unaudited).Build()
	require.NoError(t, batch.SetProto(key, &commonpb.Log{Sequence: unaudited}))
	require.NoError(t, batch.Commit())

	unauditedErrors := errorsOfType(
		collectCheckErrors(t, engine.store, engine.attrs),
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)

	require.Len(t, unauditedErrors, 1,
		"a log row above the audited maximum was written outside the audited apply "+
			"path and must be reported exactly once")
	require.Equal(t, uint64(unaudited), unauditedErrors[0].GetLogSequence())
	require.Contains(t, unauditedErrors[0].GetMessage(), "authenticates no log above 1")
}

// TestCheckDoesNotBoundLogsAfterAChainBreak pins the prefix-maximum guard. The
// audited maximum is accumulated over the chain walk, so a walk that stops at a
// break carries the maximum of a PREFIX of the history — here a break at the
// FIRST entry leaves it at 0 while the store legitimately holds two logs.
// Comparing that would report the whole stream as unaudited on top of the one
// break that is the real finding, so the pass reports only that it could not
// bound the stream.
func TestCheckDoesNotBoundLogsAfterAChainBreak(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("first"))
	engine.processAndCommit(createLedgerOrder("second"))

	// Flip the stored hash of the FIRST audit entry without re-hashing, so the
	// walk breaks before it accumulates any log range at all.
	handle, err := engine.store.NewReadHandle()
	require.NoError(t, err)

	auditKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(1).Build()

	value, closer, err := handle.Get(auditKey)
	require.NoError(t, err)

	entry := &auditpb.AuditEntry{}
	require.NoError(t, entry.UnmarshalVT(value))
	require.NoError(t, closer.Close())
	require.NoError(t, handle.Close())

	entry.Hash = append([]byte(nil), entry.GetHash()...)
	entry.Hash[0] ^= 0xFF
	rewriteAuditEntry(t, engine.store, entry, nil)

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"the prefix maximum must not be compared: both logs are audited by entries "+
			"the walk never reached, and reporting them would be a false positive")
	require.Len(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE), 1,
		"a truncated walk must report once that the log stream could not be bounded")
}

// chainedAuditEntry pairs an audit entry with its items so a whole chain can be
// persisted in one call.
type chainedAuditEntry struct {
	entry *auditpb.AuditEntry
	items []*auditpb.AuditItem
}

// persistChainedAuditEntries writes each entry at its canonical key, chaining
// every hash onto its predecessor so the whole chain verifies. persistAuditEntry
// always chains from nil and so only ever produces ONE valid entry; a
// discontinuity between two log ranges needs two entries that both verify.
func persistChainedAuditEntries(t *testing.T, store *dal.Store, clusterID string, chain ...chainedAuditEntry) {
	t.Helper()

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID)

	var lastHash []byte

	for _, link := range chain {
		headerPayload, err := state.BuildHashedHeaderPayload(link.entry)
		require.NoError(t, err)

		hashSlices := make([][]byte, 0, 1+len(link.items))
		hashSlices = append(hashSlices, headerPayload)

		for _, item := range link.items {
			hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
		}

		_, lastHash = gen.Compute(nil, lastHash, hashSlices)
		link.entry.Hash = lastHash

		rewriteAuditEntry(t, store, link.entry, link.items)
	}
}

// TestCheckDetectsDiscontinuousAuditedLogRange pins the contiguity assertion
// guarding the accumulated maximum. Each proposal allocates ONE contiguous block
// of log sequences and proposals commit in audit-sequence order, so consecutive
// log-bearing entries must abut. A hole means the maximum bounds a stream the
// chain never covered end to end, and every log inside the hole would then pass
// the bound while being authenticated by nothing.
//
// Both entries hash correctly here — an adversary that re-hashes, or an FSM bug
// in log allocation, is the only way to reach this state, which is why it is an
// invariant assertion (invariant #7) and not only a report.
func TestCheckDetectsDiscontinuousAuditedLogRange(t *testing.T) {
	t.Parallel()

	const clusterID = "test-cluster"

	store := createTestStore(t)
	attrs := attributes.New()

	serialized := createLedgerOrder("ledger").MarshalDeterministicVT(nil)

	newLink := func(auditSeq, logSeq uint64) chainedAuditEntry {
		return chainedAuditEntry{
			entry: &auditpb.AuditEntry{
				Sequence:    auditSeq,
				Timestamp:   &commonpb.Timestamp{Data: 1700000000 + auditSeq},
				ProposalId:  auditSeq,
				OrderCount:  1,
				HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
				Outcome: &auditpb.AuditEntry_Success{
					Success: &auditpb.AuditSuccess{MinLogSequence: logSeq, MaxLogSequence: logSeq},
				},
			},
			items: []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized, LogSequence: logSeq}},
		}
	}

	// Log 1 then log 5: logs 2, 3 and 4 are authenticated by nothing.
	persistChainedAuditEntries(t, store, clusterID, newLink(1, 1), newLink(2, 5))

	gaps := errorsOfType(
		collectCheckErrors(t, store, attrs),
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)

	require.Len(t, gaps, 1,
		"a jump in the audited log range must be reported exactly once")
	require.Equal(t, uint64(5), gaps[0].GetLogSequence())
	require.Contains(t, gaps[0].GetMessage(), "the audited log range is discontinuous")
}

// TestCheckDoesNotFlagHealthyContiguousLogStream is the case the whole pass is
// judged on. A bound that fires on a healthy store is worse than one that misses
// a shape: operators learn to ignore the finding, and the next real
// LOG_UNAUDITED goes to the same place as this one.
//
// The fixture is deliberately not a single proposal. Log sequences are allocated
// as one contiguous block per proposal, so a MULTI-order proposal is the shape
// that exercises the abutment assertion guarding the accumulated maximum — a
// single-order history never lands two sequences under one audit entry. It also
// mixes ledger creation, transactions, a revert and metadata, so the maximum is
// accumulated across every log-bearing order kind rather than one.
func TestCheckDoesNotFlagHealthyContiguousLogStream(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("payments"))
	engine.processAndCommit(createLedgerOrder("treasury"))

	// One proposal, three orders: log sequences 3, 4 and 5 all land under a
	// single audit entry whose range is [3, 5].
	engine.processAndCommit(
		createTransactionOrder("payments", true, newPosting("world", "alice", "USD", 500)),
		createTransactionOrder("payments", true, newPosting("world", "bob", "USD", 250)),
		createTransactionOrder("treasury", true, newPosting("world", "vault", "EUR", 1000)),
	)

	engine.processAndCommit(revertTransactionOrder("payments", 1))
	engine.processAndCommit(saveAccountMetadataOrder("payments", "alice", map[string]string{"tier": "gold"}))
	engine.processAndCommit(deleteAccountMetadataOrder("payments", "alice", "tier"))

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"every stored log was produced by an audited proposal, so the stored maximum "+
			"equals the audited maximum and nothing may be reported")
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
		"the chain walk reached the end of the live range, so the stream must be "+
			"bounded rather than declared unbounded")
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
		"consecutive log-bearing audit entries abut on a healthy store, and no log "+
			"sequence is missing from the stream")
	require.Empty(t, errs, "a healthy multi-proposal store must be reported entirely clean")
}

// sealArchivedChapter fills in the sealing hash the way the sealer does, so
// verifySealingHash accepts the chapter. Recomputed here rather than copied from
// a fixture constant because it binds the chapter's own id, close sequence and
// hashes — see verifySealingHash for the decomposition (and log_bounds.go for
// why this hash being UNKEYED is the reason archiveEndSeq must never raise the
// log bound).
func sealArchivedChapter(p *commonpb.Chapter) {
	hasher := blake3.New()
	buf := make([]byte, 8)

	binary.BigEndian.PutUint64(buf, p.GetId())
	_, _ = hasher.Write(buf)

	binary.BigEndian.PutUint64(buf, p.GetCloseSequence())
	_, _ = hasher.Write(buf)

	if len(p.GetLastAuditHash()) > 0 {
		_, _ = hasher.Write(p.GetLastAuditHash())
	}

	_, _ = hasher.Write(p.GetStateHash())

	p.SealingHash = hasher.Sum(nil)
}

// archivedBoundary is the chapter metadata a purge boundary is written with.
// closeSequence is the last LOG sequence of the archived chapter and becomes
// signing.archiveEndSeq; closeAuditSequence is the last AUDIT sequence and is
// where the live chain walk starts. The two ride independent counters and are
// not interchangeable.
type archivedBoundary struct {
	closeSequence      uint64
	closeAuditSequence uint64
	// lastAuditHash is the hash of the entry AT closeAuditSequence, the chain
	// input the first surviving entry is verified against.
	lastAuditHash []byte
	// purgeLogRows mirrors executePurge deleting the log rows in the archived
	// range. False leaves them retained, which out-of-order archiving does.
	purgeLogRows bool
}

// writeArchivedBoundary seals an ARCHIVED chapter over [1, closeSequence],
// opens the next chapter after it, and purges the archived range exactly the
// way WriteSet.executePurge does — logs by log sequence, audit entries and
// applied proposals by audit sequence. Audit ITEM rows are deliberately left
// behind: executePurge does not delete them either.
func writeArchivedBoundary(t *testing.T, engine *testEngine, boundary archivedBoundary) {
	t.Helper()

	archived := &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		Start:              &commonpb.Timestamp{Data: 1700000001},
		End:                &commonpb.Timestamp{Data: 1700000002},
		StartSequence:      1,
		CloseSequence:      boundary.closeSequence,
		StartAuditSequence: 1,
		CloseAuditSequence: boundary.closeAuditSequence,
		LastAuditHash:      boundary.lastAuditHash,
		StateHash:          make([]byte, 32),
	}
	sealArchivedChapter(archived)

	open := &commonpb.Chapter{
		Id:                 2,
		Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
		Start:              &commonpb.Timestamp{Data: 1700000003},
		StartSequence:      boundary.closeSequence + 1,
		StartAuditSequence: boundary.closeAuditSequence + 1,
	}

	batch := engine.store.OpenWriteSession()
	require.NoError(t, state.StoreChapter(batch, archived))
	require.NoError(t, state.StoreChapter(batch, open))
	require.NoError(t, state.StoreNextChapterID(batch, 3))

	if boundary.purgeLogRows {
		require.NoError(t, batch.DeleteRange(
			dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(archived.GetStartSequence()).Build(),
			dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(archived.GetCloseSequence()+1).Build(),
			nil))
	}

	require.NoError(t, batch.DeleteRange(
		dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(archived.GetStartAuditSequence()).Build(),
		dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(archived.GetCloseAuditSequence()+1).Build(),
		nil))
	require.NoError(t, batch.DeleteRange(
		dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(archived.GetStartAuditSequence()).Build(),
		dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(archived.GetCloseAuditSequence()+1).Build(),
		nil))
	require.NoError(t, batch.Commit())
}

// captureBoundaryBaseline takes the baseline checkpoint the way archival does:
// AT the boundary, before any post-boundary apply, so it is an independent
// record of the purged range rather than a copy of what is being verified.
// Without it Check() abandons entry-by-entry verification on an archived store
// (the baselineDB == nil return), which would make every later assertion vacuous.
func captureBoundaryBaseline(t *testing.T, engine *testEngine) {
	t.Helper()

	handle, err := engine.store.NewReadHandle()
	require.NoError(t, err)

	baselinePath, err := engine.store.BaselineSnapshotDir()
	require.NoError(t, err)
	require.NoError(t, attributes.CreateBaselineSnapshot(handle, baselinePath))
	require.NoError(t, handle.Close())
}

// TestCheckBoundsLogsAcrossAnArchivedBoundary covers the archived store, which
// is where the bound has the most room to be wrong in both directions:
//
//   - expectedLogMax only ever accumulates over the LIVE audit range (the walk
//     starts after the highest archived close-audit-sequence), so it never
//     accounts for the purged range;
//   - storedLogMax is taken BEFORE the `seq <= archiveEndSeq` skip, so it does
//     count any pre-boundary row the purge left behind.
//
// A naive comparison would therefore report a healthy archived store. It does
// not, because the archive flow emits its own logs above the range it purges, so
// live audit entries always cover the highest stored sequence. The fixture
// mirrors that: proposals continue after the boundary, exactly as the
// Seal/Archive/ConfirmArchive proposals do in production.
//
// Both purge outcomes are covered. The retained variant is the out-of-order
// archiving shape the replay loop clips at `seq <= archiveEndSeq`, and it is the
// one the deliberately-rejected max(expectedLogMax, archiveEndSeq) clamp was
// meant to "fix" — it needs no clamp, because those rows sit below the live
// maximum the chain already authenticates.
//
// Each variant then asserts the other direction on the SAME store: an injected
// row above the audited maximum is still reported. Silence alone would prove
// nothing here, because Check() abandons entry-by-entry verification outright on
// an archived store with no baseline checkpoint (see the `baselineDB == nil`
// return) — and compareLogBounds runs after that point, so a fixture that lost
// its baseline would go quiet for the wrong reason.
func TestCheckBoundsLogsAcrossAnArchivedBoundary(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// purgePreBoundaryLogs mirrors executePurge deleting the log rows in the
		// archived range. False leaves them retained, which out-of-order
		// archiving legitimately does.
		purgePreBoundaryLogs bool
	}{
		{name: "pre-boundary logs purged", purgePreBoundaryLogs: true},
		{name: "pre-boundary logs retained", purgePreBoundaryLogs: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := newTestEngine(t)

			// Pre-boundary history: logs 1-2, audit entries 1-2.
			engine.processAndCommit(createLedgerOrder("archived-era"))
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "alice", "USD", 100)))

			// The chain input for the first surviving entry is the hash of the
			// entry at the close-audit-sequence, which the chapter carries.
			boundaryAuditHash := append([]byte(nil), engine.lastAuditHash...)

			captureBoundaryBaseline(t, engine)

			// Post-boundary history: logs 3-4, audit entries 3-4. Stands in for
			// the archive flow's own proposals, which is why a live entry always
			// authenticates the highest stored log.
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "bob", "USD", 50)))
			engine.processAndCommit(createLedgerOrder("live-era"))

			writeArchivedBoundary(t, engine, archivedBoundary{
				closeSequence:      2,
				closeAuditSequence: 2,
				lastAuditHash:      boundaryAuditHash,
				purgeLogRows:       tc.purgePreBoundaryLogs,
			})

			healthy := collectCheckErrors(t, engine.store, engine.attrs)

			require.Empty(t, errorsOfType(healthy, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
				"the live audit range authenticates the highest stored log, so an "+
					"archived boundary must not make the bound fire")
			require.Empty(t, errorsOfType(healthy, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
				"the live walk chained from the chapter's last audit hash and reached "+
					"the end of the range, so the stream is bounded")
			require.Empty(t, errorsOfType(healthy, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
				"the first live entry has nothing to abut and the replay starts at "+
					"archiveEndSeq + 1, so neither the audited range nor the stream has a hole")

			// The other direction, on the same store: the bound is actually
			// running, not skipped along with the rest of the verification.
			const unaudited = 5

			inject := engine.store.OpenWriteSession()
			key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(unaudited).Build()
			require.NoError(t, inject.SetProto(key, &commonpb.Log{Sequence: unaudited}))
			require.NoError(t, inject.Commit())

			unauditedErrors := errorsOfType(
				collectCheckErrors(t, engine.store, engine.attrs),
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)

			require.Len(t, unauditedErrors, 1,
				"a row above the audited maximum must be reported on an archived store too")
			require.Equal(t, uint64(unaudited), unauditedErrors[0].GetLogSequence())
			require.Contains(t, unauditedErrors[0].GetMessage(), "authenticates no log above 4")
		})
	}
}

// mirrorLedgerOrder creates a ledger in MIRROR mode, the only mode
// processMirrorIngest accepts.
func mirrorLedgerOrder(name string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{
						Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
					},
				},
			},
		},
	}
}

// mirrorFillGapOrder ingests one source v2 log that has no v3 equivalent. The
// simplest ingest kind: it mutates no transaction, so re-submitting it exercises
// nothing but the contiguous-applied-prefix guard.
func mirrorFillGapOrder(name string, v2LogID uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Entry: &raftcmdpb.MirrorLogEntry{
							V2LogId: v2LogID,
							Data: &raftcmdpb.MirrorLogEntry_FillGap{
								FillGap: &raftcmdpb.MirrorFillGap{},
							},
						},
					},
				},
			},
		},
	}
}

// TestCheckDoesNotBoundLogsBelowAnIdempotentMirrorReplay pins the max-take that
// raises the audited ceiling. A proposal whose every order was an idempotent
// mirror replay produces NO log — processMirrorIngest returns (nil, nil) on
// `v2LogID <= last`, and ProcessOrders consumes no sequence id for it — so its
// audit entry carries the {0, 0} range.
//
// That entry is the LAST one here on purpose. Assigning the range instead of
// taking the maximum would leave the audited ceiling at 0 with nothing after it
// to raise it again, and every log the store legitimately holds would be
// reported as written outside the audited apply path. The zero range must also
// not trip the abutment assertion, which is why that assertion skips
// `minLogSeq == 0`.
//
// The replay is driven through the real processing pipeline rather than
// hand-written, so the {0, 0} entry is the one the FSM actually produces.
func TestCheckDoesNotBoundLogsBelowAnIdempotentMirrorReplay(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(mirrorLedgerOrder("mirror"))

	applied := engine.processAndCommit(mirrorFillGapOrder("mirror", 1))
	require.Len(t, applied, 1, "the first ingest of v2 log 1 must apply and create a log")

	replayed := engine.processAndCommit(mirrorFillGapOrder("mirror", 1))
	require.Empty(t, replayed,
		"re-ingesting an already-applied v2 log must be a no-log outcome, which is "+
			"what makes its audit entry carry the {0, 0} range this test is about")

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"a trailing {0, 0} audit entry must not pull the audited maximum back down "+
			"and report the whole stream as unaudited")
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
		"a no-log proposal does not truncate the chain walk")
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
		"the {0, 0} range has no minimum to abut the previous maximum, so it must "+
			"not be read as a hole in the audited range")
	require.Empty(t, errs, "an idempotent mirror replay leaves the store clean")
}

// appendChainedAuditEntry writes one more audit entry onto the tail of the chain
// the engine already built, hashing it from the engine's last hash so the whole
// chain still verifies. The engine only ever appends SUCCESS entries (it drives
// ProcessOrders, which returns an error rather than a rejection), so a failure
// tail has to be written here. Sequence, hash version and hash are filled in;
// everything else comes from the caller.
func appendChainedAuditEntry(t *testing.T, engine *testEngine, entry *auditpb.AuditEntry, items []*auditpb.AuditItem) {
	t.Helper()

	entry.Sequence = engine.nextAuditSequenceID
	entry.HashVersion = uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3)

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, engine.clusterID)

	headerPayload, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(t, err)

	hashSlices := make([][]byte, 0, 1+len(items))
	hashSlices = append(hashSlices, headerPayload)

	for _, item := range items {
		hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
	}

	_, entry.Hash = gen.Compute(nil, engine.lastAuditHash, hashSlices)

	rewriteAuditEntry(t, engine.store, entry, items)

	engine.lastAuditHash = entry.GetHash()
	engine.nextAuditSequenceID++
}

// TestCheckDoesNotBoundLogsBelowTrailingFailures pins the other end of the
// history that cannot move the audited maximum: a store whose last proposals
// were all REJECTED. The failure path writes the audit entry and returns before
// any log is appended, so a failure entry carries no AuditSuccess at all and the
// maximum is simply not touched — the audited ceiling stays where the last
// successful proposal left it, which is exactly the highest stored log.
//
// The tail matters because it is the shape EN-1526 already got wrong once: the
// deleted `lastSequence == 0` gate skipped the whole chain walk on a
// failure-only store. A bound that read the LAST entry rather than the running
// maximum would repeat that mistake from the other side, reporting every log in
// the store as unaudited because the final entry authenticates none.
func TestCheckDoesNotBoundLogsBelowTrailingFailures(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("ledger"))
	engine.processAndCommit(createTransactionOrder("ledger", true,
		newPosting("world", "alice", "USD", 10)))

	// The order a rejected proposal carries. Its item gets LogSequence 0, which
	// is what buildAuditItems produces for a failure.
	rejected := createTransactionOrder("ledger", false,
		newPosting("alice", "bob", "USD", 999999))
	serialized := rejected.MarshalDeterministicVT(nil)

	for i := range 2 {
		appendChainedAuditEntry(t, engine, &auditpb.AuditEntry{
			Timestamp:  &commonpb.Timestamp{Data: 1700000100 + uint64(i)},
			ProposalId: engine.raftIndex + uint64(i),
			OrderCount: 1,
			Outcome: &auditpb.AuditEntry_Failure{
				Failure: &auditpb.AuditFailure{
					Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
					Message: "balance too low",
				},
			},
		}, []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized}})
	}

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"a failure entry produces no log and carries no AuditSuccess, so it cannot "+
			"lower the audited maximum below the logs the successful entries produced")
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
		"the failure entries hash correctly, so the walk is not truncated")
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
		"a failure allocates no log sequence, so the audited range stays contiguous")
	require.Empty(t, errs, "a store ending in rejected proposals must be reported clean")
}

// TestCheckReportsDiscontinuityAboveAnArchivedBoundary covers the abutment
// assertion's interaction with an archive boundary, on a store where
// signing.archiveEndSeq is non-zero rather than trivially 0.
//
// The archived chapter closes at log 2, so a live entry jumping from the audited
// maximum 3 straight to 6 is a hole ABOVE the boundary, and it is reported. A
// boundary must not become a blanket excuse that swallows a hole in the audited
// range.
//
// The assertion is deliberately not gated on archiveEndSeq at all. It once
// carried a `minLogSeq > signing.archiveEndSeq` conjunct, on the theory that a
// boundary needed excusing; that was wrong in both directions. It never excused
// anything reachable, because what actually legalises the first visited entry is
// `chainBound.expectedLogMax > 0` — the maximum is still 0 there, with nothing to
// abut. And it could only ever SUPPRESS: close_sequence overlapping the live
// audited range is corruption, not out-of-order archiving, and close_sequence is
// the forgeable field (unkeyed sealing hash) while min/max_log_sequence are
// inside the keyed pre-image. Gating a keyed-hash finding on a forgeable field is
// the EN-1526 defect shape, so the conjunct is gone; see log_bounds.go.
//
// Note the counter asymmetry that makes the old theory fail, since it is easy to
// re-derive backwards: close_sequence is the CloseChapter log's OWN sequence
// while close_audit_sequence is one BELOW its own audit entry. The walk starts at
// close_audit_sequence + 1, i.e. at that chapter's own CloseChapter entry, whose
// min_log_sequence therefore sits at or below archiveEndSeq — so a comparison
// against archiveEndSeq would not have excused the first entry either.
func TestCheckReportsDiscontinuityAboveAnArchivedBoundary(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Pre-boundary history: logs 1-2, audit entries 1-2.
	engine.processAndCommit(createLedgerOrder("archived-era"))
	engine.processAndCommit(createTransactionOrder("archived-era", true,
		newPosting("world", "alice", "USD", 100)))

	boundaryAuditHash := append([]byte(nil), engine.lastAuditHash...)

	captureBoundaryBaseline(t, engine)

	// Two live entries, both correctly chained onto the boundary hash so the
	// walk reaches the second one: log 3, then a jump to log 6. Logs 4 and 5 are
	// authenticated by nothing. Only an adversary that re-hashes, or an FSM bug
	// in log allocation, produces this — hence the invariant assertion.
	serialized := createLedgerOrder("live-era").MarshalDeterministicVT(nil)

	for _, logSeq := range []uint64{3, 6} {
		appendChainedAuditEntry(t, engine, &auditpb.AuditEntry{
			Timestamp:  &commonpb.Timestamp{Data: 1700000010 + logSeq},
			ProposalId: 100 + logSeq,
			OrderCount: 1,
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{MinLogSequence: logSeq, MaxLogSequence: logSeq},
			},
		}, []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized, LogSequence: logSeq}})
	}

	writeArchivedBoundary(t, engine, archivedBoundary{
		closeSequence:      2,
		closeAuditSequence: 2,
		lastAuditHash:      boundaryAuditHash,
		purgeLogRows:       true,
	})

	gaps := errorsOfType(
		collectCheckErrors(t, engine.store, engine.attrs),
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)

	require.Len(t, gaps, 1,
		"a jump in the audited log range above the archive boundary must still be "+
			"reported: archiveEndSeq narrows which ranges are held to the assertion, "+
			"it must not excuse a hole above itself")
	require.Equal(t, uint64(6), gaps[0].GetLogSequence())
	require.Contains(t, gaps[0].GetMessage(), "the audited log range is discontinuous")
	require.Contains(t, gaps[0].GetMessage(), "authenticates logs from 6 but the highest previously audited log was 3")
}

// TestCheckBoundsRetainedLogsUnderTheArchiveBoundary pins WHERE the stored
// ceiling is raised in the replay loop: before the `seq <= archiveEndSeq` skip,
// not after it.
//
// Every other fixture keeps the highest stored log ABOVE the boundary, so the
// row that sets storedLogMax never meets the skip and both placements agree.
// This one puts the whole log stream at or under the boundary. close_sequence is
// 5 while the live audited range only reaches log 4, which is not out-of-order
// archiving but the forgeable shape log_bounds.go names: the sealing hash is
// UNKEYED, so whoever edits close_sequence just recomputes it
// (sealArchivedChapter does exactly that here) and verifySealingHash still
// accepts the chapter.
//
// Raising the ceiling after the skip would let that forged close_sequence hide
// the injected row: every stored key would be skipped, storedLogMax would stay
// 0, and the one pass that can see a log no proposal produced would compare
// nothing. That is the archiveEndSeq-gated threshold compareLogBounds exists to
// refuse, reached through the replay loop instead of through the comparison.
//
// The retained-only variant is the control: it proves the finding in the other
// variant comes from the injected row and not from the forged boundary itself.
func TestCheckBoundsRetainedLogsUnderTheArchiveBoundary(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// injectAtBoundary writes one log row AT close_sequence, above the live
		// audited maximum and therefore authenticated by nothing.
		injectAtBoundary bool
		wantUnaudited    bool
	}{
		{name: "retained logs only", injectAtBoundary: false, wantUnaudited: false},
		{name: "row injected at the boundary", injectAtBoundary: true, wantUnaudited: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := newTestEngine(t)

			// Pre-boundary history: logs 1-2, audit entries 1-2.
			engine.processAndCommit(createLedgerOrder("archived-era"))
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "alice", "USD", 100)))

			boundaryAuditHash := append([]byte(nil), engine.lastAuditHash...)

			captureBoundaryBaseline(t, engine)

			// Post-boundary history: logs 3-4, audit entries 3-4. The live walk
			// starts at audit 3, so the audited maximum is 4.
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "bob", "USD", 50)))
			engine.processAndCommit(createLedgerOrder("live-era"))

			// close_sequence 5 covers the whole retained stream and one sequence
			// beyond it, while close_audit_sequence stays at 2 so the live chain
			// walk still verifies. Log rows are retained, so nothing below the
			// boundary is deleted either.
			writeArchivedBoundary(t, engine, archivedBoundary{
				closeSequence:      5,
				closeAuditSequence: 2,
				lastAuditHash:      boundaryAuditHash,
				purgeLogRows:       false,
			})

			if tc.injectAtBoundary {
				const unaudited = 5

				inject := engine.store.OpenWriteSession()
				key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(unaudited).Build()
				require.NoError(t, inject.SetProto(key, &commonpb.Log{Sequence: unaudited}))
				require.NoError(t, inject.Commit())
			}

			// A forged boundary clips the replay above the whole live range, so
			// the run legitimately reports volume, transaction, boundary,
			// unaudited-ledger and signing-incomplete findings as well. Only the
			// bound is under test here, hence the type filter.
			unauditedErrors := errorsOfType(
				collectCheckErrors(t, engine.store, engine.attrs),
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)

			if !tc.wantUnaudited {
				require.Empty(t, unauditedErrors,
					"the highest retained row is the audited maximum, so the bound must "+
						"stay silent: the finding in the other variant comes from the "+
						"injected row, not from the forged close_sequence")

				return
			}

			require.Len(t, unauditedErrors, 1,
				"a row at close_sequence is above the audited maximum and must be "+
					"reported exactly once: the stored ceiling has to be raised before "+
					"the archive-boundary skip, or a forged close_sequence suppresses it")
			require.Equal(t, uint64(5), unauditedErrors[0].GetLogSequence())
			require.Contains(t, unauditedErrors[0].GetMessage(), "authenticates no log above 4")
			require.Contains(t, unauditedErrors[0].GetMessage(), "extends 1 sequence(s) past the audited maximum")
		})
	}
}

// TestCheckStillJudgesBaselineLessArchivedStores pins the two passes that keep
// running when an archived store has no readable baseline checkpoint.
//
// Check() abandons entry-by-entry verification on that shape — without the
// baseline there is no independent pre-archive state, so every per-ledger
// projection comparison would run on a partial view. But two expectations are
// already whole at that point: signing.foldArchived and verifyAuditHashChain ran
// before it and neither reads the baseline. Both are cluster-global rather than
// per-ledger, so a store can hold signing rows and log rows the audit never
// produced while every baseline-seeded term is legitimately empty.
//
// Base covered this through the `lastSequence == 0` fast path, which called the
// signing compare before the archived branch was reached. That path is gone, so
// the two passes are pinned here instead: returning without them reports clean on
// exactly the injected-key and injected-log classes they exist to catch.
func TestCheckStillJudgesBaselineLessArchivedStores(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// tamper plants one unaudited log row above the audited maximum and one
		// signing row too short to decode.
		tamper bool
	}{
		{name: "untampered", tamper: false},
		{name: "unaudited log and undecodable signing row", tamper: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := newTestEngine(t)

			// Pre-boundary history: logs 1-2, audit entries 1-2.
			engine.processAndCommit(createLedgerOrder("archived-era"))
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "alice", "USD", 100)))

			boundaryAuditHash := append([]byte(nil), engine.lastAuditHash...)

			// Deliberately NO captureBoundaryBaseline: this is the checkpoint-only
			// restore shape, where BaselineCheckpointPath finds nothing and Check()
			// takes the baselineDB == nil return.

			// Post-boundary history: logs 3-4, audit entries 3-4, so the audited
			// maximum is 4 and the retained stream stops there too.
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "bob", "USD", 50)))
			engine.processAndCommit(createLedgerOrder("live-era"))

			writeArchivedBoundary(t, engine, archivedBoundary{
				closeSequence:      2,
				closeAuditSequence: 2,
				lastAuditHash:      boundaryAuditHash,
				purgeLogRows:       true,
			})

			if tc.tamper {
				const unaudited = 5

				inject := engine.store.OpenWriteSession()
				key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(unaudited).Build()
				require.NoError(t, inject.SetProto(key, &commonpb.Log{Sequence: unaudited}))
				require.NoError(t, inject.Commit())

				// The undecodable class is the one signing finding that needs no
				// audit oracle, so it is reported whether or not the cold fold was
				// complete — which makes it the observable that proves the compare
				// ran at all on a store with no cold reader attached.
				writeRawSigningKeyRow(t, engine.store, "forged-key", []byte{0x01})
			}

			errs := collectCheckErrors(t, engine.store, engine.attrs)

			unauditedErrors := errorsOfType(errs,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)
			signingErrors := errorsOfType(errs,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH)

			if !tc.tamper {
				require.Empty(t, unauditedErrors,
					"the retained stream stops at the audited maximum, so the bound must stay silent")
				require.Empty(t, signingErrors,
					"no signing row was planted, so the compare must find nothing")

				// Everything this healthy store reports must be a coverage gap
				// rather than a divergence. The distinction is load-bearing off
				// the end of this package: ValidateRestore forwards findings to
				// `ledgerctl restore validate`, which counts the two apart and
				// lets --allow-incomplete accept gaps but never a divergence.
				// Since no cold reader is attached here — nor on either
				// production caller — a divergence escaping this fixture means a
				// valid backup can no longer be validated at all.
				for _, e := range errs {
					require.True(t, IsCoverageGap(e.GetErrorType()),
						"a healthy baseline-less archived store must report only "+
							"incomplete coverage, never a divergence; got %s: %s",
						e.GetErrorType(), e.GetMessage())
				}

				require.NotEmpty(t, errs,
					"the signing expectation cannot be completed without a cold reader, "+
						"so the run must SAY so rather than fall silent")

				// The projection comparisons were skipped wholesale, and that has to
				// reach the STREAM, not just the server log. Consumers that build a
				// verdict see only this channel, so a log-only skip let
				// `restore validate` report a clean backup over a dozen passes that
				// never ran. Pinned here because the finding is the operator's only
				// signal that "the audit chain checks out" is not "the projections
				// were verified".
				archivedGaps := errorsOfType(errs,
					servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ARCHIVED_STATE_VERIFICATION_INCOMPLETE)
				require.Len(t, archivedGaps, 1,
					"the baseline-less archived skip must be reported on the event stream")
				require.Contains(t, archivedGaps[0].GetMessage(), "UNVERIFIED",
					"the message must name the skipped passes as unverified, not merely "+
						"mention that a baseline was missing")

				return
			}

			require.Len(t, unauditedErrors, 1,
				"the log bound must still run without a baseline: it needs only the "+
					"audited maximum from the chain walk and the highest stored key")
			require.Equal(t, uint64(5), unauditedErrors[0].GetLogSequence())
			require.Contains(t, unauditedErrors[0].GetMessage(), "authenticates no log above 4")

			require.Len(t, signingErrors, 1,
				"the signing compare must still run without a baseline: its expectation "+
					"is folded from archived and live audit orders, never from the baseline")
			require.Contains(t, signingErrors[0].GetMessage(), "forged-key")
			require.Contains(t, signingErrors[0].GetMessage(), "undecodable stored row")
		})
	}
}

// runCheckCollecting runs Check() and returns both the error events and the
// error, instead of asserting the latter away.
//
// collectCheckErrors requires NoError, which is the right default everywhere
// else: a malformed row is normally a finding, not a checker failure. A log key
// that cannot be decoded is the exception — the pass cannot name the sequence
// the row claims, so it can neither replay it nor bound it and has to abort.
func runCheckCollecting(t *testing.T, store *dal.Store, attrs *attributes.Attributes) ([]*servicepb.CheckStoreError, error) {
	t.Helper()

	checker := NewChecker(store, attrs, "test-cluster", nil, nil, nil, logging.Testing())

	var errs []*servicepb.CheckStoreError

	err := checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			errs = append(errs, e.Error)
		}
	})

	return errs, err
}

// writeShortLogKey plants a log-prefix row whose key is too short to hold a
// sequence.
//
// Such a key is INSIDE both log iterators' range and sorts above every
// realistic sequence. The iterators run from the bare two-byte zone prefix to a
// ten-byte all-0xFF key, so a short key filled toward 0xFF is a strict prefix of
// the upper bound; and because "shorter is less" only breaks ties on a shared
// prefix, byte 2 decides — 0x01 here against the 0x00 that every real sequence
// below 2^56 starts with. So it is the last key in the range, which is where
// readStoredLogMax's reverse seek lands and where the replay loop's forward walk
// ends.
//
// Four bytes rather than the minimum two on purpose: the bare prefix is the one
// short key that sorts FIRST, so it would exercise neither position.
func writeShortLogKey(t *testing.T, store *dal.Store) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(
		[]byte{dal.ZoneCold, dal.SubColdLog, 0x01, 0xFF},
		&commonpb.Log{Sequence: 1}))
	require.NoError(t, batch.Commit())
}

// TestCheckRejectsMalformedLogKeys pins the length check on the log-key decode.
//
// Both decode sites slice the sequence out of the key, and Go bounds-checks a
// slice expression against CAPACITY rather than length. Pebble hands back keys
// from an internal buffer, so an unchecked decode of a short key is not reliably
// a panic: it is a panic OR eight adjacent buffer bytes read as a sequence,
// decided by allocation state. The fabricated-sequence half is the worse one —
// in the replay loop it feeds `for expectedSeq < seq`, which on a near-2^64
// value emits SEQUENCE_GAP until the run is killed.
//
// Every repository writer goes through KeyBuilder.PutUint64 and emits exactly
// ten bytes, so this row is Pebble-level corruption or direct store access.
// It is still in scope: ValidateRestore runs Check() over an untrusted foreign
// staged backup (see the checker construction in
// RestoreServiceServerImpl.ValidateRestore, which is also the caller that passes
// no cold reader).
//
// Both decode sites are covered because they are reached on DISJOINT paths, and
// fixing only one leaves the more damaging half in place:
//
//   - the replay loop, on an ordinary store;
//   - readStoredLogMax, on the baseline-less archived store, where Check()
//     abandons entry-by-entry verification and the replay loop never runs.
func TestCheckRejectsMalformedLogKeys(t *testing.T) {
	t.Parallel()

	t.Run("replay loop", func(t *testing.T) {
		t.Parallel()

		engine := newTestEngine(t)

		engine.processAndCommit(createLedgerOrder("ledger"))
		engine.processAndCommit(createTransactionOrder("ledger", true,
			newPosting("world", "alice", "USD", 100)))

		writeShortLogKey(t, engine.store)

		errs, err := runCheckCollecting(t, engine.store, engine.attrs)

		require.ErrorContains(t, err, "is 4 bytes, want 10",
			"a log key too short to hold a sequence must abort the replay with a "+
				"contextual error, not decode to whatever follows it in Pebble's buffer")
		require.Empty(t,
			errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
			"the bound must not report a fabricated sequence as an unaudited log")
		require.Empty(t,
			errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
			"the gap emission loop must never be reached with a fabricated sequence: "+
				"on a near-2^64 value it does not terminate")
	})

	t.Run("baseline-less archived store", func(t *testing.T) {
		t.Parallel()

		engine := newTestEngine(t)

		// Pre-boundary history: logs 1-2, audit entries 1-2.
		engine.processAndCommit(createLedgerOrder("archived-era"))
		engine.processAndCommit(createTransactionOrder("archived-era", true,
			newPosting("world", "alice", "USD", 100)))

		boundaryAuditHash := append([]byte(nil), engine.lastAuditHash...)

		// No captureBoundaryBaseline: this is the checkpoint-only restore shape,
		// where Check() takes the baselineDB == nil return and readStoredLogMax is
		// the only reader of the log keys.
		engine.processAndCommit(createTransactionOrder("archived-era", true,
			newPosting("world", "bob", "USD", 50)))
		engine.processAndCommit(createLedgerOrder("live-era"))

		writeArchivedBoundary(t, engine, archivedBoundary{
			closeSequence:      2,
			closeAuditSequence: 2,
			lastAuditHash:      boundaryAuditHash,
			purgeLogRows:       true,
		})

		writeShortLogKey(t, engine.store)

		errs, err := runCheckCollecting(t, engine.store, engine.attrs)

		require.ErrorContains(t, err, "is 4 bytes, want 10",
			"readStoredLogMax reverse-seeks straight onto the short key, so the "+
				"baseline-less archived path has to reject it too")
		require.Empty(t,
			errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
			"a fabricated ceiling read off the buffer would report the whole live "+
				"stream as unaudited")
	})
}

// TestCheckReportsDiscontinuityUnderAnArchivedBoundary is the mirror of
// TestCheckReportsDiscontinuityAboveAnArchivedBoundary, and it exists to pin the
// one thing that fixture cannot: that the abutment assertion is independent of
// signing.archiveEndSeq.
//
// The guard once carried a `minLogSeq > signing.archiveEndSeq &&` conjunct. Every
// other fixture in this file keeps its hole ABOVE the boundary, where both
// variants of the guard fire, so the conjunct could be re-added with the whole
// package green -- and re-adding it is not a hypothetical refactor, it is the
// change this PR made, argued at length above compareLogBounds and above the
// guard itself.
//
// Here the audited range jumps from log 3 to log 6 while close_sequence is 6, so
// the jump lands AT the boundary rather than above it. HEAD reports the
// discontinuity; the conjunct suppresses it entirely. That is the whole point:
// close_sequence is covered only by the UNKEYED sealing hash, which
// sealArchivedChapter recomputes here exactly as an attacker would, while
// min/max_log_sequence sit inside the keyed audit pre-image. Letting the
// forgeable field gate the keyed-hash finding is the EN-1526 defect shape rebuilt
// inside the assertion meant to catch it.
//
// close_sequence 6 against close_audit_sequence 2 is a tampered shape, not
// out-of-order archiving -- which is correct for this checker's threat model, and
// one number away from what TestCheckBoundsRetainedLogsUnderTheArchiveBoundary
// already ships. purgeLogRows is false so the rows in the claimed range survive,
// as they would under a forged boundary.
func TestCheckReportsDiscontinuityUnderAnArchivedBoundary(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Pre-boundary history: logs 1-2, audit entries 1-2.
	engine.processAndCommit(createLedgerOrder("archived-era"))
	engine.processAndCommit(createTransactionOrder("archived-era", true,
		newPosting("world", "alice", "USD", 100)))

	boundaryAuditHash := append([]byte(nil), engine.lastAuditHash...)

	captureBoundaryBaseline(t, engine)

	// Two live entries, correctly chained so the walk reaches the second: log 3,
	// then a jump to log 6. Logs 4 and 5 are authenticated by nothing.
	serialized := createLedgerOrder("live-era").MarshalDeterministicVT(nil)

	for _, logSeq := range []uint64{3, 6} {
		appendChainedAuditEntry(t, engine, &auditpb.AuditEntry{
			Timestamp:  &commonpb.Timestamp{Data: 1700000010 + logSeq},
			ProposalId: 100 + logSeq,
			OrderCount: 1,
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{MinLogSequence: logSeq, MaxLogSequence: logSeq},
			},
		}, []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized, LogSequence: logSeq}})
	}

	// close_sequence 6 puts the whole discontinuity at or below archiveEndSeq,
	// which is the only configuration in which the deleted conjunct changes the
	// outcome. close_audit_sequence stays 2 so the live walk still starts below
	// the two entries above and verifies them.
	writeArchivedBoundary(t, engine, archivedBoundary{
		closeSequence:      6,
		closeAuditSequence: 2,
		lastAuditHash:      boundaryAuditHash,
		purgeLogRows:       false,
	})

	gaps := errorsOfType(
		collectCheckErrors(t, engine.store, engine.attrs),
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)

	require.Len(t, gaps, 1,
		"a hole in the audited log range must be reported even when a forged "+
			"close_sequence reaches over it: archiveEndSeq is covered by an unkeyed "+
			"hash and must never gate a keyed-hash finding")
	require.Equal(t, uint64(6), gaps[0].GetLogSequence())
	require.Contains(t, gaps[0].GetMessage(), "the audited log range is discontinuous")
	require.Contains(t, gaps[0].GetMessage(), "authenticates logs from 6 but the highest previously audited log was 3")
}

// TestCheckDetectsALostLogTailThroughTheBoundaryPass pins the detection
// compareLogBounds formally defers to.
//
// compareLogBounds is max-only by design and says so: a lost TAIL above the
// highest stored key is invisible both to it and to the replay loop's gap
// detection, which runs INSIDE the iteration and therefore stops at the last
// stored row. The comment above compareLogBounds does not merely describe that
// division of labour, it justifies NOT adding a third finding for the opposite
// direction -- and audit-chain.md repeats the guarantee. The delegate is a single
// entry in compareBoundaries' field list, `{"nextLogId", ...}`, and nothing
// asserted on it: deleting that one line left the package green.
//
// The fixture is a NO-OP metadata tail on purpose, not a transaction tail. On an
// ordinary transaction tail four other passes also fire, so the deletion is
// masked and a count assertion is unstable. Re-setting a metadata key to the
// value it already holds is entirely ordinary traffic whose projection effect is
// nil, so the surviving prefix replays to byte-identical projections and this one
// field is the SOLE detector. That single-finding property is itself what is
// worth pinning.
func TestCheckDetectsALostLogTailThroughTheBoundaryPass(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("ledger"))
	engine.processAndCommit(saveAccountMetadataOrder("ledger", "alice", map[string]string{"k": "v"}))
	// Same key, same value: the log exists and moves NextLogId, but folding it
	// changes no projection, so dropping it leaves every other pass silent.
	engine.processAndCommit(saveAccountMetadataOrder("ledger", "alice", map[string]string{"k": "v"}))

	require.Empty(t, collectCheckErrors(t, engine.store, engine.attrs),
		"the fixture must be clean before the tail is removed, or the assertion "+
			"below cannot attribute its finding to the deletion")

	// Drop the log row alone. The audit entry that authenticates it stays, so the
	// audited maximum still reaches 3 while the replay only reaches 2 -- the
	// mirror image of the LOG_UNAUDITED direction, and the per-type
	// backup-segment shape.
	batch := engine.store.OpenWriteSession()
	require.NoError(t, batch.DeleteKey(
		dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(3).Build()))
	require.NoError(t, batch.Commit())

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Len(t, errs, 1,
		"a lost log tail whose payload changed no projection is detectable through "+
			"the replayed boundary expectation and nowhere else")
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_BOUNDARY_MISMATCH,
		errs[0].GetErrorType())
	require.Contains(t, errs[0].GetMessage(), "boundary field nextLogId mismatch: stored 3, expected 2")
}

// TestBaselineLessArchivedLeavesProjectionsUnverified pins the residual coverage
// hole rather than hiding it.
//
// Without a baseline the projection comparisons cannot run at all: the baseline
// is the checker's only independent source of pre-archive state, and backfilling
// it from the live store would verify the data against a copy of itself. So a
// corrupted Volume row on this shape goes unreported, and the run says so
// instead of reporting clean — which is the whole point of the archived-state
// finding.
//
// The subtests are a matched pair. The baseline-present control proves the
// tampered row IS detectable, so the baseline-absent case is measuring the
// missing baseline and not a fixture that forgot to corrupt anything. Delete
// either half and the remaining one stops meaning what it claims.
//
// This is a limitation barrier, not an approval: if the baseline ever becomes
// available on the restore path, the absent case starts reporting VOLUME_MISMATCH
// and this test must be rewritten rather than relaxed.
func TestBaselineLessArchivedLeavesProjectionsUnverified(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// withBaseline captures the boundary baseline, giving compareVolumes the
		// independent pre-archive expectation it needs.
		withBaseline bool
	}{
		{name: "baseline absent - projections unverified", withBaseline: false},
		{name: "baseline present - corruption detected", withBaseline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := newTestEngine(t)

			// Pre-boundary history: logs 1-2.
			engine.processAndCommit(createLedgerOrder("archived-era"))
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "alice", "USD", 100)))

			boundaryAuditHash := append([]byte(nil), engine.lastAuditHash...)

			if tc.withBaseline {
				captureBoundaryBaseline(t, engine)
			}

			// Post-boundary history: logs 3-4. bob/USD is credited 50 here, so its
			// volume row is inside the post-archive delta either way.
			engine.processAndCommit(createTransactionOrder("archived-era", true,
				newPosting("world", "bob", "USD", 50)))
			engine.processAndCommit(createLedgerOrder("live-era"))

			writeArchivedBoundary(t, engine, archivedBoundary{
				closeSequence:      2,
				closeAuditSequence: 2,
				lastAuditHash:      boundaryAuditHash,
				purgeLogRows:       true,
			})

			// Inflate bob's input from the audited 50 to 999.
			batch := engine.store.OpenWriteSession()
			tamperedKey := domain.VolumeKey{
				AccountKey: domain.AccountKey{LedgerName: "archived-era", Account: "bob"},
				Asset:      "USD",
			}
			_, err := engine.attrs.Volume.Set(batch, tamperedKey.Bytes(), &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(999),
				Output: commonpb.NewUint256FromUint64(0),
			})
			require.NoError(t, err)
			require.NoError(t, batch.Commit())

			errs := collectCheckErrors(t, engine.store, engine.attrs)

			volumeErrors := errorsOfType(errs,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH)
			archivedGaps := errorsOfType(errs,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_ARCHIVED_STATE_VERIFICATION_INCOMPLETE)

			if tc.withBaseline {
				require.Len(t, volumeErrors, 1,
					"with a baseline the volume comparison runs and must catch the inflated input")
				require.Contains(t, volumeErrors[0].GetMessage(), "999")
				require.Empty(t, archivedGaps,
					"the archived state WAS compared, so nothing may claim it was skipped")

				return
			}

			require.Empty(t, volumeErrors,
				"without a baseline compareVolumes cannot run, so the corruption is "+
					"genuinely undetected on this shape — if this starts failing the "+
					"baseline became available and this test needs rewriting, not relaxing")

			require.Len(t, archivedGaps, 1,
				"the run must report that the projections were not verified; reporting "+
					"nothing here is what let `restore validate` certify this store, and "+
					"is what its exit status now keys off")

			// The gap must not be mistaken for a divergence by any consumer that
			// builds a verdict: it says a pass did not run, not that the store is
			// wrong. The corruption above is real, but this run did not establish it.
			require.True(t, IsCoverageGap(archivedGaps[0].GetErrorType()))
		})
	}
}
