package check

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// errorsOfType filters a Check() run down to one error type. The fixtures below
// inject rows that legitimately trip other passes as well (a payload-less log
// row also reaches the elision guard), so pinning the log-bounds findings needs
// the type filter rather than a total count.
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
// assertion's interaction with an archive boundary — the eighth case, added
// after case 7 was found to exercise the assertion only with
// signing.archiveEndSeq == 0.
//
// The fixture has a real boundary: the archived chapter closes at log 2, so
// signing.archiveEndSeq is 2, not 0, and the `minLogSeq > signing.archiveEndSeq`
// conjunct is genuinely evaluated rather than trivially satisfied. A live entry
// then jumps from the audited maximum 3 straight to 6, and the finding is still
// reported. That is the half that protects the audit: a boundary must not become
// a blanket excuse that swallows a hole in the audited range.
//
// It does NOT pin the conjunct's excusing direction, and no test can, because
// that direction is unreachable on a store whose chapter metadata is internally
// consistent. close_sequence and close_audit_sequence are both assigned at
// CloseChapter apply time from monotonic counters, so across archived chapters
// they rise together and the SAME chapter supplies both maxima. The live walk
// starts after the highest archived close_audit_sequence, so every entry it
// visits was applied after that chapter closed, and therefore carries
// minLogSeq > that chapter's close_sequence == signing.archiveEndSeq. The
// scenario the conjunct was written for — an un-archived chapter below the
// boundary — is in fact handled by the `chainBound.expectedLogMax > 0` guard
// instead: the skipped span leaves the maximum at 0 at the first visited entry.
//
// Reaching the excuse requires an archived close_sequence that overlaps the live
// audited range, which is corruption rather than out-of-order archiving — and
// close_sequence is the attacker-forgeable field (unkeyed sealing hash, see
// log_bounds.go). Pinning that as expected behaviour would bless a forgeable
// suppression of an invariant finding, so this test pins the reachable half and
// the unreachable half is reported instead.
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
