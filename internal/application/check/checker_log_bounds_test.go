package check

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// logRowKey builds the Pebble key of one Log row. The bounds pass reads the
// stored head off the KEY, so the fixtures below have to address rows the same
// way rather than through the value's `sequence` field.
func logRowKey(sequence uint64) []byte {
	key := make([]byte, 10)
	key[0] = dal.ZoneHistory
	key[1] = dal.SubHistoryLog
	binary.BigEndian.PutUint64(key[2:], sequence)

	return key
}

// highestStoredLogKey returns the greatest Log KEY sequence in the store, and
// how many Log rows there are. 0 rows means a highest of 0.
func highestStoredLogKey(t *testing.T, store *dal.Store) (uint64, int) {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: logRowKey(0),
		UpperBound: logPrefixUpperBound(),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	var (
		highest uint64
		rows    int
	)

	for iter.First(); iter.Valid(); iter.Next() {
		highest = binary.BigEndian.Uint64(iter.Key()[2:10])
		rows++
	}

	require.NoError(t, iter.Error())

	return highest, rows
}

// deleteLogRowsAbove drops every Log row whose key sequence is greater than
// keep, leaving the audit chain untouched. That is the restore failure mode the
// bounds pass exists to catch: the surviving prefix is internally consistent,
// so no other pass has an oracle for the missing rows.
func deleteLogRowsAbove(t *testing.T, store *dal.Store, keep uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, batch.DeleteRange(
		logRowKey(keep+1),
		logPrefixUpperBound(),
		nil,
	))
	require.NoError(t, batch.Commit())
}

// writeRawLogRows writes Log rows for every sequence in [from, to], with the
// value's `sequence` field equal to the key so the key/value agreement check
// (CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH) stays silent. The payload is
// left nil: these fixtures exercise the log BOUND, and a nil payload is skipped
// by the replay switch without contributing expectations of its own.
func writeRawLogRows(t *testing.T, store *dal.Store, from, to uint64) {
	t.Helper()

	require.LessOrEqual(t, from, to, "the fixture range must ascend")

	batch := store.OpenWriteSession()

	// Post-tested rather than `sequence <= to`: a fixture writing the row at
	// math.MaxUint64 — the top-of-key-space case the bound must now cover —
	// would wrap that condition to 0 and never terminate.
	for sequence := from; ; sequence++ {
		require.NoError(t, batch.SetProto(logRowKey(sequence), &commonpb.Log{Sequence: sequence}))

		if sequence == to {
			break
		}
	}

	require.NoError(t, batch.Commit())
}

// persistSuccessAuditEntries writes one chain-valid SUCCESS audit entry per
// given [min, max] log range, chained from genesis. Ranges are written verbatim
// so a fixture can express a discontinuity no production path can produce.
//
// Each entry gets one AuditItem per position in its range, matching what
// buildAuditItems emits on a real apply — one item per order, carrying that
// order's fresh log sequence. Without them the fixture would express a shape no
// writer can produce (a range no order accounts for), which
// logBoundsVerifier.observeSuccess now rejects on its own; every fixture built
// here would then report LOG_VERIFICATION_INCOMPLETE before reaching the
// property it exists to pin. Use persistItemlessSuccessAuditEntry to build that
// shape deliberately.
func persistSuccessAuditEntries(t *testing.T, store *dal.Store, ranges [][2]uint64) {
	t.Helper()

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, auditOnlyClusterID)

	var (
		lastHash    []byte
		hashScratch []byte
	)

	for i, logRange := range ranges {
		sequence := uint64(i + 1)
		items := successRangeAuditItems(logRange[0], logRange[1])

		entry := &auditpb.AuditEntry{
			Sequence:    sequence,
			Timestamp:   &commonpb.Timestamp{Data: 1700000000 + sequence},
			ProposalId:  sequence,
			HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
			OrderCount:  uint32(len(items)),
			Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{
					MinLogSequence: logRange[0],
					MaxLogSequence: logRange[1],
				},
			},
		}

		headerPayload, err := state.BuildHashedHeaderPayload(entry)
		require.NoError(t, err)

		hashSlices := make([][]byte, 0, 1+len(items))
		hashSlices = append(hashSlices, headerPayload)

		for _, item := range items {
			hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
		}

		hashScratch, entry.Hash = gen.Compute(hashScratch, lastHash, hashSlices)
		lastHash = entry.GetHash()

		rewriteAuditEntry(t, store, entry, items)
	}
}

// successRangeAuditItems builds the per-order items a real apply would have
// written for a success covering [minSeq, maxSeq]: one item per fresh log, in
// ascending order. An empty range (0..0) produces no item, which is the log-less
// success shape.
//
// Ranges wider than a handful of logs are capped: the only fixture that needs a
// wide one is the large-truncation test, and materialising thousands of items to
// pin an aggregated event would slow the suite for no added coverage. Above the
// cap the items still cover the range's endpoints and everything between them up
// to the cap, which is enough for the coverage check, because that check compares
// the DISTINCT in-range item sequences against the range width — so a capped
// fixture would be rejected. Callers needing a wide range therefore get every
// position; the cap only guards against an accidental 2^64 fixture.
func successRangeAuditItems(minSeq, maxSeq uint64) []*auditpb.AuditItem {
	if maxSeq == 0 || minSeq > maxSeq {
		return nil
	}

	const maxFixtureItems = 100_000

	width := maxSeq - minSeq + 1
	if width > maxFixtureItems {
		panic(fmt.Sprintf("fixture success range %d..%d spans %d logs, above the %d-item cap; "+
			"build the entry with persistItemlessSuccessAuditEntry or a narrower range",
			minSeq, maxSeq, width, maxFixtureItems))
	}

	// OrderIndex is the item row's key suffix, so it has to be distinct or the
	// rows collide and only the last one survives the write.
	items := make([]*auditpb.AuditItem, 0, width)
	for logSeq := minSeq; logSeq <= maxSeq; logSeq++ {
		items = append(items, &auditpb.AuditItem{
			OrderIndex:  uint32(logSeq - minSeq),
			LogSequence: logSeq,
		})
	}

	return items
}

// engineAuditEntry is the input of appendEngineAuditEntry.
type engineAuditEntry struct {
	engine *testEngine
	// entry needs only its Outcome set; the sequence, timestamp, proposal id,
	// hash version and hash are filled from the engine's chain cursor.
	entry *auditpb.AuditEntry
	items []*auditpb.AuditItem
}

// appendEngineAuditEntry chains one hand-built audit entry onto the history a
// testEngine has produced so far, then advances the engine's chain cursor so a
// later processAndCommit keeps hashing forward from it. That is what lets a
// fixture interleave an entry the processing pipeline cannot produce — a
// rejected proposal, or a success that created no log — into an otherwise real
// history.
func appendEngineAuditEntry(t *testing.T, in engineAuditEntry) {
	t.Helper()

	engine := in.engine

	in.entry.Sequence = engine.nextAuditSequenceID
	in.entry.Timestamp = &commonpb.Timestamp{Data: 1700000000 + engine.raftIndex}
	in.entry.ProposalId = engine.raftIndex
	in.entry.HashVersion = uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3)
	in.entry.OrderCount = uint32(len(in.items))

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, engine.clusterID)

	headerPayload, err := state.BuildHashedHeaderPayload(in.entry)
	require.NoError(t, err)

	hashSlices := make([][]byte, 0, 1+len(in.items))
	hashSlices = append(hashSlices, headerPayload)

	for _, item := range in.items {
		hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
	}

	_, in.entry.Hash = gen.Compute(nil, engine.lastAuditHash, hashSlices)

	rewriteAuditEntry(t, engine.store, in.entry, in.items)

	engine.lastAuditHash = in.entry.GetHash()
	engine.nextAuditSequenceID++
	engine.raftIndex++
}

// errorsOfType returns every collected event carrying the given type.
func errorsOfType(errs []*servicepb.CheckStoreError, want servicepb.CheckStoreErrorType) []*servicepb.CheckStoreError {
	var out []*servicepb.CheckStoreError

	for _, e := range errs {
		if e.GetErrorType() == want {
			out = append(out, e)
		}
	}

	return out
}

// referenceTransactionOrder builds a CreateTransaction carrying a reference,
// optionally opting into the reference-conflict skip. A second order with the
// same reference and the skip declared is converted to an OrderSkipped log by
// the real processor, which is the second of the two log-sequence producing
// call sites (internal/domain/processing/processor.go).
func referenceTransactionOrder(ledger, reference string, skippable bool, postings ...*commonpb.Posting) *raftcmdpb.Order {
	var reasons []commonpb.ErrorReason
	if skippable {
		reasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}
	}

	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Postings:  postings,
								Force:     true,
								Reference: reference,
							},
						},
						SkippableReasons: reasons,
					},
				},
			},
		},
	}
}

// requireNoLogBoundFindings asserts the three bound-pass classes are silent.
// The tail-gap event is distinguished from the interior per-sequence scan by
// its message, which names a range rather than a single sequence.
func requireNoLogBoundFindings(t *testing.T, errs []*servicepb.CheckStoreError) {
	t.Helper()

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
		"the log bound must be derivable over a healthy history, got %v", errs)
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"no stored log may sit above the audited bound, got %v", errs)

	for _, e := range errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP) {
		require.NotContains(t, e.GetMessage(), "the audit chain accounts for logs up to",
			"the bounds pass must report no tail gap, got %q", e.GetMessage())
	}
}

// TestCheck_LogBounds_HealthyHistoryIsContiguous pins the premise the whole
// pass rests on, independently of the checker: over a chain-verified range the
// union of the AuditSuccess [min, max] ranges is one contiguous interval
// starting at 1, and its upper end is the highest stored log key.
//
// The fixture is deliberately the widest the pipeline allows — several ledgers,
// transactions, metadata writes and deletes, a revert, a ledger deletion, an
// order the processor converts to a skip (the second log-producing call site),
// plus an interleaved rejected proposal (which writes an audit entry and no
// log) and a success that created no log at all.
func TestCheck_LogBounds_HealthyHistoryIsContiguous(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("bounds"))
	engine.processAndCommit(createLedgerOrder("scratch"))

	engine.processAndCommit(createTransactionOrder("bounds", true,
		newPosting("world", "bank", "USD", 100000),
	))
	engine.processAndCommit(createTransactionOrder("bounds", false,
		newPosting("bank", "user:alice", "USD", 2500),
	))

	// A rejected proposal in the middle of the history: an audit entry with a
	// Failure outcome, one item per order with LogSequence 0, and no Log row.
	// It must neither advance the bound nor interrupt the interval.
	failureEntry, failureItems := newFailureAuditEntry(0, 2)
	appendEngineAuditEntry(t, engineAuditEntry{engine: engine, entry: failureEntry, items: failureItems})

	engine.processAndCommit(saveAccountMetadataOrder("bounds", "user:alice", map[string]string{
		"status": "active",
		"tier":   "gold",
	}))
	engine.processAndCommit(deleteAccountMetadataOrder("bounds", "user:alice", "tier"))

	// A reference claim, then the same reference with the conflict skip
	// declared: the processor rolls the second order back and emits an
	// OrderSkipped log, which consumes a log sequence like any other.
	engine.processAndCommit(referenceTransactionOrder("bounds", "ref-1", false,
		newPosting("world", "user:bob", "USD", 700),
	))
	engine.processAndCommit(referenceTransactionOrder("bounds", "ref-1", true,
		newPosting("world", "user:bob", "USD", 700),
	))

	// A success that created no log: an all-idempotent proposal reports
	// min == max == 0 and must be ignored by the bound.
	appendEngineAuditEntry(t, engineAuditEntry{
		engine: engine,
		entry: &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{},
		}},
	})

	engine.processAndCommit(revertTransactionOrder("bounds", 2))
	engine.processAndCommit(deleteLedgerOrder("scratch"))

	// --- the premise, read straight off the audit entries ---
	handle, err := engine.store.NewReadHandle()
	require.NoError(t, err)

	cursor, err := query.ReadAuditEntries(context.Background(), handle, nil)
	require.NoError(t, err)

	var (
		expectedMax uint64
		successes   int
		failures    int
		noLog       int
	)

	for {
		entry, cursorErr := cursor.Next()
		if cursorErr != nil {
			require.ErrorIs(t, cursorErr, io.EOF)

			break
		}

		success := entry.GetSuccess()
		if success == nil {
			failures++

			continue
		}

		if success.GetMaxLogSequence() == 0 {
			noLog++

			continue
		}

		successes++

		require.Equal(t, expectedMax+1, success.GetMinLogSequence(),
			"audit entry %d must start exactly where the previous success range ended", entry.GetSequence())
		require.LessOrEqual(t, success.GetMinLogSequence(), success.GetMaxLogSequence(),
			"audit entry %d has an inverted log range", entry.GetSequence())

		expectedMax = success.GetMaxLogSequence()
	}

	require.NoError(t, cursor.Close())
	require.NoError(t, handle.Close())

	require.Equal(t, 1, failures, "the fixture must carry the rejected proposal")
	require.Equal(t, 1, noLog, "the fixture must carry the log-less success")
	require.Greater(t, successes, 5, "the fixture must carry a real history")

	storedMax, rows := highestStoredLogKey(t, engine.store)
	require.EqualValues(t, expectedMax, storedMax,
		"the audited bound must equal the highest stored log key")
	require.EqualValues(t, storedMax, rows,
		"a contiguous interval from 1 means one row per sequence")

	// --- and the checker agrees ---
	errs := collectCheckErrors(t, engine.store, engine.attrs)
	requireNoLogBoundFindings(t, errs)
	require.Empty(t, errs, "a healthy history must produce no check errors at all")
}

// TestCheck_LogBounds_DeletedTailReportsOneGap is the pass's reason to exist. A
// store that lost its top Log rows but kept its audit chain is internally
// consistent: every projection rebuilt from the surviving prefix agrees with
// it, the interior gap scan has no surviving row above the hole, and before
// this pass the whole run reported nothing.
func TestCheck_LogBounds_DeletedTailReportsOneGap(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		keep    uint64
		missing uint64
	}{
		{"two rows deleted", 4, 2},
		{"four rows deleted", 2, 4},
		{"every row deleted", 0, 6},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := newTestEngine(t)
			engine.processAndCommit(createLedgerOrder("truncated"))

			for i := range 5 {
				engine.processAndCommit(createTransactionOrder("truncated", true,
					newPosting("world", "user:alice", "USD", int64(100*(i+1))),
				))
			}

			storedMax, _ := highestStoredLogKey(t, engine.store)
			require.EqualValues(t, 6, storedMax, "the fixture must hold six logs")

			require.Empty(t, collectCheckErrors(t, engine.store, engine.attrs),
				"baseline before truncation must be clean")

			deleteLogRowsAbove(t, engine.store, tc.keep)

			survivingMax, _ := highestStoredLogKey(t, engine.store)
			require.Equal(t, tc.keep, survivingMax)

			errs := collectCheckErrors(t, engine.store, engine.attrs)

			gaps := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)
			require.Len(t, gaps, 1,
				"a deleted tail must be reported once, not once per missing sequence, got %v", errs)
			require.Equal(t, tc.keep+1, gaps[0].GetLogSequence(),
				"the event must name the first missing sequence")

			message := gaps[0].GetMessage()
			require.Contains(t, message, fmt.Sprintf("log sequences %d..%d are missing", tc.keep+1, 6),
				"the message must name the whole missing range, got %q", message)
			require.Contains(t, message, fmt.Sprintf("(%d logs)", tc.missing),
				"the message must count the missing rows, got %q", message)

			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED))
			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
				"the chain is intact, so the bound is derivable, got %v", errs)
		})
	}
}

// TestCheck_LogBounds_RestoreLostTailIsOtherwiseInvisible is the failure mode
// the pass was built for, in the shape it actually takes. A cross-cluster
// restore that loses the top Log rows rebuilds every projection from the
// surviving prefix, so volumes, transaction states and boundaries all agree
// with the logs that are left. The audit chain still accounts for the lost
// range, and it is the ONLY thing that does: the assertion below is that the
// tail gap is the single finding of the whole run.
func TestCheck_LogBounds_RestoreLostTailIsOtherwiseInvisible(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("restored"))
	engine.processAndCommit(createTransactionOrder("restored", true,
		newPosting("world", "bank", "USD", 5000),
	))
	engine.processAndCommit(createTransactionOrder("restored", false,
		newPosting("bank", "user:alice", "USD", 900),
	))

	storedMax, _ := highestStoredLogKey(t, engine.store)
	require.EqualValues(t, 3, storedMax, "the surviving prefix must hold three logs")
	require.Empty(t, collectCheckErrors(t, engine.store, engine.attrs),
		"the surviving prefix and its projections must be self-consistent")

	// The audit tail the restore kept: three chain-valid success entries whose
	// log ranges continue past the highest surviving row. Nothing else in the
	// store mentions those logs.
	//
	// Each carries the per-order item a real apply would have written for the
	// log it created. Without it the entry would declare a range no order
	// accounts for, and observeSuccess would suppress the bound instead of
	// deriving it — the fixture would then pin nothing.
	for sequence := uint64(4); sequence <= 6; sequence++ {
		appendEngineAuditEntry(t, engineAuditEntry{
			engine: engine,
			entry: &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{MinLogSequence: sequence, MaxLogSequence: sequence},
			}},
			items: successRangeAuditItems(sequence, sequence),
		})
	}

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Len(t, errs, 1,
		"the lost tail must be reported, and by this pass alone: every other projection agrees "+
			"with the surviving logs, got %v", errs)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP, errs[0].GetErrorType())
	require.EqualValues(t, 4, errs[0].GetLogSequence())
	require.Contains(t, errs[0].GetMessage(), "log sequences 4..6 are missing")
	require.Contains(t, errs[0].GetMessage(), "(3 logs)")
}

// TestCheck_LogBounds_InjectedLogAboveAuditedRange covers the reverse
// direction: a Log row nothing in the audit chain produced. Log rows are not
// hash-bound, so the audited ranges are the only statement of which positions
// the FSM ever allocated.
func TestCheck_LogBounds_InjectedLogAboveAuditedRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		injected uint64
	}{
		{"one row above the bound", 5},
		{"three rows above the bound", 7},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := newTestEngine(t)
			engine.processAndCommit(createLedgerOrder("injected"))

			for i := range 3 {
				engine.processAndCommit(createTransactionOrder("injected", true,
					newPosting("world", "user:alice", "USD", int64(100*(i+1))),
				))
			}

			audited, _ := highestStoredLogKey(t, engine.store)
			require.EqualValues(t, 4, audited)

			// The value's sequence field matches the key, so the key/value
			// agreement check stays silent and only the bound can catch this.
			writeRawLogRows(t, engine.store, audited+1, tc.injected)

			errs := collectCheckErrors(t, engine.store, engine.attrs)

			unaudited := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)
			require.Len(t, unaudited, 1,
				"an injected tail must be reported once for the whole range, got %v", errs)
			require.Equal(t, audited+1, unaudited[0].GetLogSequence())
			require.Contains(t, unaudited[0].GetMessage(),
				fmt.Sprintf("log sequences %d..%d have no audited origin", audited+1, tc.injected))
			require.Contains(t, unaudited[0].GetMessage(),
				fmt.Sprintf("(%d logs)", tc.injected-audited))

			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
				"the injected rows are contiguous with the audited prefix, got %v", errs)
			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH),
				"the injected rows agree with their keys, got %v", errs)
			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE))
		})
	}
}

// TestCheck_LogBounds_ChainBreakAboveTruncationSuppressesGap is the regression
// test for the event flood that sank the first attempt at this pass (#1666).
// When the chain breaks, the derived bound is a prefix of the real one: every
// surviving log above the break reads as unaudited and the gap below it is
// arbitrary. The pass must say it could not derive the bound instead of
// reporting either.
func TestCheck_LogBounds_ChainBreakAboveTruncationSuppressesGap(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("break"))

	for i := range 5 {
		engine.processAndCommit(createTransactionOrder("break", true,
			newPosting("world", "user:alice", "USD", int64(100*(i+1))),
		))
	}

	storedMax, _ := highestStoredLogKey(t, engine.store)
	require.EqualValues(t, 6, storedMax)

	// Lose the top three logs, then break the chain on the audit entry whose
	// success range sits above what survived.
	deleteLogRowsAbove(t, engine.store, 3)

	handle, err := engine.store.NewReadHandle()
	require.NoError(t, err)

	entry, err := query.ReadAuditEntry(context.Background(), handle, 5)
	require.NoError(t, err)
	require.NoError(t, handle.Close())
	require.NotNil(t, entry)
	require.EqualValues(t, 5, entry.GetSuccess().GetMinLogSequence(),
		"the tampered entry must be one whose logs are gone")

	entry.ProposalId += 1000
	rewriteAuditEntry(t, engine.store, entry, nil)

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Len(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH), 1,
		"the walk stops at the first break, got %v", errs)

	incomplete := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE)
	require.Len(t, incomplete, 1, "exactly one coverage finding, never one per sequence, got %v", errs)
	require.Contains(t, incomplete[0].GetMessage(), "cut short by a hash chain break")

	for _, e := range errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP) {
		require.LessOrEqual(t, e.GetLogSequence(), uint64(3),
			"no gap may be derived above the surviving head from a partial bound, got %q", e.GetMessage())
	}

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"a partial bound must not report surviving logs as injected, got %v", errs)
}

// TestCheck_LogBounds_FailureOnlyHistory covers the zero-log end of the range:
// a history of nothing but rejected proposals has a complete chain, no success
// range at all, and no Log row. Expected and stored bounds are both 0.
func TestCheck_LogBounds_FailureOnlyHistory(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	persistFailureOnlyHistory(t, store, 3, 2)

	storedMax, rows := highestStoredLogKey(t, store)
	require.Zero(t, storedMax)
	require.Zero(t, rows)

	requireNoLogBoundFindings(t, collectCheckErrors(t, store, attributes.New()))
}

// TestCheck_LogBounds_ReservedSequenceZeroIsUnaudited pins the LOWER end of the
// audited interval, which logBoundsVerifier.compare does not bound: it checks
// the stored head against expectedMax, and a row at sequence 0 moves neither
// side — on a healthy store the head stays at the real maximum, and on a store
// where the forged row is the only one both sides read 0, indistinguishable
// from an empty store. Every other pass misses it too: the interior gap scan is
// seeded at expectedSeq 1 and never looks beneath it, and the row's own
// `sequence` field can agree with its key.
//
// Sequence 0 is nonetheless a position no proposal can allocate
// (FSMState.NextSequenceID is seeded at 1 and recovery only raises it, and
// observeSuccess pins the first audited range's minimum at 1), so the row is
// unaudited by construction. The log loop reports it off the key.
func TestCheck_LogBounds_ReservedSequenceZeroIsUnaudited(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		setup func(t *testing.T, store *dal.Store)
		// storedMax is the highest log KEY the store holds once set up, which is
		// what compare() is handed — unmoved by the forged row in both shapes.
		storedMax uint64
		rows      int
	}{
		{
			name: "alongside a healthy audited history",
			setup: func(t *testing.T, store *dal.Store) {
				writeRawLogRows(t, store, 1, 4)
				persistSuccessAuditEntries(t, store, [][2]uint64{{1, 4}})
			},
			storedMax: 4,
			rows:      5,
		},
		{
			name: "as the store's only log row",
			setup: func(t *testing.T, store *dal.Store) {
				persistFailureOnlyHistory(t, store, 3, 2)
			},
			storedMax: 0,
			rows:      1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			tc.setup(t, store)

			// The value's sequence field matches the key, so the key/value
			// agreement check stays silent, and the payload is nil, so replay
			// contributes no expectation of its own. Only the reserved-position
			// check can catch this row.
			writeRawLogRows(t, store, 0, 0)

			storedMax, rows := highestStoredLogKey(t, store)
			require.Equal(t, tc.storedMax, storedMax,
				"the forged row must not move the stored head compare() is handed")
			require.Equal(t, tc.rows, rows, "the forged row must be present in the store")

			errs := collectCheckErrors(t, store, attributes.New())

			require.Len(t, errs, 1, "the forged row is the only divergence in either fixture, got %v", errs)

			unaudited := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)
			require.Len(t, unaudited, 1,
				"a log row at reserved sequence 0 must be reported exactly once, got %v", errs)
			require.Zero(t, unaudited[0].GetLogSequence())
			require.Contains(t, unaudited[0].GetMessage(), "log sequence 0 has no audited origin",
				"the single-position message must be distinguishable from compare()'s range message")

			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
				"sequence 0 sits below the audited interval, it is not a hole inside it, got %v", errs)
			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH),
				"the forged row agrees with its key, so only the reserved position can catch it, got %v", errs)
			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE),
				"the audit chain is intact in both fixtures, got %v", errs)
		})
	}
}

// TestCheck_LogBounds_LogLessSuccessIsIgnored pins the one success shape that
// carries no log range: an all-idempotent proposal reports min == max == 0. It
// must neither advance the bound nor break the contiguity of the interval
// around it, so it sits between two log-producing ranges here.
func TestCheck_LogBounds_LogLessSuccessIsIgnored(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeRawLogRows(t, store, 1, 4)
	persistSuccessAuditEntries(t, store, [][2]uint64{{1, 2}, {0, 0}, {3, 4}})

	storedMax, _ := highestStoredLogKey(t, store)
	require.EqualValues(t, 4, storedMax)

	requireNoLogBoundFindings(t, collectCheckErrors(t, store, attributes.New()))
}

// TestCheck_LogBounds_DiscontinuousSuccessRangesFailLoudly covers the branch
// that is unreachable by contract: the audited ranges skip a sequence. The
// producer allocates log sequences contiguously from a counter no other path
// advances, so this cannot happen on a real store — which is exactly why
// invariant #7 forbids swallowing it. It is reported, the bound derived from
// the broken premise is suppressed, and Check() still returns without error.
//
// The interior per-sequence gap scan is independent of the bound and still
// reports the hole it can see, so the two findings are asserted separately.
func TestCheck_LogBounds_DiscontinuousSuccessRangesFailLoudly(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	// Rows at 1, 2, 4, 5 — no row at 3, which the interior scan sees.
	writeRawLogRows(t, store, 1, 2)
	writeRawLogRows(t, store, 4, 5)

	// Chain-valid success entries whose ranges skip sequence 3.
	persistSuccessAuditEntries(t, store, [][2]uint64{{1, 2}, {4, 5}})

	errs := collectCheckErrors(t, store, attributes.New())

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH),
		"the fixture's chain is valid, got %v", errs)

	incomplete := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE)
	require.Len(t, incomplete, 1, "exactly one invariant finding, got %v", errs)
	require.True(t, strings.HasPrefix(incomplete[0].GetMessage(), "invariant:"),
		"an unreachable-by-contract branch must be loud, got %q", incomplete[0].GetMessage())
	require.Contains(t, incomplete[0].GetMessage(), "audit entry 2 declares log range 4..5")
	require.Contains(t, incomplete[0].GetMessage(), "before it end at 2")

	// The interior scan's own finding: sequence 3 has no row, and a surviving
	// row above it makes the hole visible without any audit oracle.
	gaps := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)
	require.Len(t, gaps, 1, "only the interior scan may report here, got %v", errs)
	require.EqualValues(t, 3, gaps[0].GetLogSequence())
	require.Equal(t, "log sequence 3 is missing", gaps[0].GetMessage(),
		"the bounds pass must contribute no gap event on a suppressed bound")

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
		"nothing may be derived from a premise that just failed, got %v", errs)
}

// TestCheck_LogBounds_InvertedSuccessRangeFailsLoudly covers the other range
// shape no producer can emit: a minimum above the maximum. It is separated
// from the genuinely log-less success (min == max == 0) only by the ORDER the
// two shapes are tested in, so `min > 0, max == 0` is the case that regresses
// first — a log-less short-circuit placed ahead of the inversion test reads
// that entry as "this proposal created no log" and drops the inversion in
// silence, which contradicts both this file's header and checker.md.
//
// The two findings must stay tellable apart, so the discontinuity wording is
// asserted absent: an inversion reported as a hole would send a reader looking
// for a missing sequence that does not exist.
func TestCheck_LogBounds_InvertedSuccessRangeFailsLoudly(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		min  uint64
		max  uint64
		want string
	}{
		{
			name: "max zero under a nonzero min",
			min:  3,
			max:  0,
			want: "audit entry 2 declares an inverted log range 3..0",
		},
		{
			name: "min above a nonzero max",
			min:  5,
			max:  4,
			want: "audit entry 2 declares an inverted log range 5..4",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)

			// A healthy [1, 2] range first, so the fold reaches the inverted
			// entry with expectedMax == 2 and the stored rows stay contiguous:
			// every event below is then attributable to the inversion alone.
			writeRawLogRows(t, store, 1, 2)
			persistSuccessAuditEntries(t, store, [][2]uint64{{1, 2}, {tc.min, tc.max}})

			storedMax, rows := highestStoredLogKey(t, store)
			require.EqualValues(t, 2, storedMax)
			require.Equal(t, 2, rows)

			errs := collectCheckErrors(t, store, attributes.New())

			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH),
				"the fixture's chain is valid, got %v", errs)

			incomplete := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE)
			require.Len(t, incomplete, 1, "exactly one invariant finding, got %v", errs)
			require.True(t, strings.HasPrefix(incomplete[0].GetMessage(), "invariant:"),
				"an unreachable-by-contract branch must be loud, got %q", incomplete[0].GetMessage())
			require.Contains(t, incomplete[0].GetMessage(), tc.want)
			require.NotContains(t, incomplete[0].GetMessage(), "before it end at",
				"an inversion must not be reported as a discontinuity, got %q", incomplete[0].GetMessage())

			// Logs 1..2 are exactly what the healthy range accounts for. The
			// suppressed bound may report them in neither direction.
			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
				"nothing may be derived from a premise that just failed, got %v", errs)
			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
				"the stored logs are contiguous and audited, got %v", errs)
		})
	}
}

// TestCheck_LogBounds_LargeTruncationStaysBounded pins the aggregation: the
// event count must not scale with the number of missing rows. A cross-cluster
// restore that loses a tail loses millions of rows, and one event per sequence
// is what made the first attempt unusable.
func TestCheck_LogBounds_LargeTruncationStaysBounded(t *testing.T) {
	t.Parallel()

	const (
		total = 3000
		keep  = 5
	)

	store := createTestStore(t)
	writeRawLogRows(t, store, 1, total)
	persistSuccessAuditEntries(t, store, [][2]uint64{{1, total}})

	storedMax, rows := highestStoredLogKey(t, store)
	require.EqualValues(t, total, storedMax)
	require.Equal(t, total, rows)
	requireNoLogBoundFindings(t, collectCheckErrors(t, store, attributes.New()))

	deleteLogRowsAbove(t, store, keep)

	survivingMax, survivingRows := highestStoredLogKey(t, store)
	require.EqualValues(t, keep, survivingMax)
	require.Equal(t, keep, survivingRows)

	errs := collectCheckErrors(t, store, attributes.New())

	gaps := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)
	require.Len(t, gaps, 1, "%d missing rows must still be one event, got %d", total-keep, len(gaps))
	require.EqualValues(t, keep+1, gaps[0].GetLogSequence())
	require.Contains(t, gaps[0].GetMessage(),
		fmt.Sprintf("log sequences %d..%d are missing", keep+1, total))
	require.Contains(t, gaps[0].GetMessage(), fmt.Sprintf("(%d logs)", total-keep))

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED))
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE))
}

// persistItemlessSuccessAuditEntry writes ONE chain-valid SUCCESS audit entry
// declaring [minSeq, maxSeq] with no AuditItem rows at all — the shape a
// writer cannot produce, where a range claims log positions no order accounts
// for. persistSuccessAuditEntries deliberately does not build it; this helper
// exists so the coverage check can be pinned against it.
func persistItemlessSuccessAuditEntry(t *testing.T, store *dal.Store, minSeq, maxSeq uint64) {
	t.Helper()

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, auditOnlyClusterID)

	entry := &auditpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &commonpb.Timestamp{Data: 1700000001},
		ProposalId:  1,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{MinLogSequence: minSeq, MaxLogSequence: maxSeq},
		},
	}

	headerPayload, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(t, err)

	_, entry.Hash = gen.Compute(nil, nil, [][]byte{headerPayload})

	rewriteAuditEntry(t, store, entry, nil)
}

// TestCheck_LogBounds_SparseInjectedTailCountsRowsNotWidth pins the count on
// the unaudited side against the rows that are actually there.
//
// The width of the interval and the number of rows in it coincide only for a
// contiguous injection, which is the shape
// TestCheck_LogBounds_InjectedLogAboveAuditedRange covers. A sparse injection
// separates them, and reporting the width there both over-counts and claims the
// empty positions hold unaudited rows — positions the interior gap scan reports,
// correctly, as missing in the very same run. Two co-emitted findings would then
// contradict each other.
func TestCheck_LogBounds_SparseInjectedTailCountsRowsNotWidth(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("sparse"))

	for i := range 3 {
		engine.processAndCommit(createTransactionOrder("sparse", true,
			newPosting("world", "user:alice", "USD", int64(100*(i+1))),
		))
	}

	audited, _ := highestStoredLogKey(t, engine.store)
	require.EqualValues(t, 4, audited)

	// One forged row at 7, nothing at 5 or 6. The value's sequence field matches
	// the key, so the key/value agreement check stays silent.
	writeRawLogRows(t, engine.store, 7, 7)

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	unaudited := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)
	require.Len(t, unaudited, 1, "the injected row must be reported once, got %v", errs)
	require.EqualValues(t, 5, unaudited[0].GetLogSequence())
	require.Contains(t, unaudited[0].GetMessage(), "(1 logs)",
		"the count must be the rows observed above the bound, not the width of the range they sit in, got %q",
		unaudited[0].GetMessage())

	// And the empty positions beneath the forged row are NOT reported as
	// missing. Nothing allocated them: the chain accounts for logs up to 4, so 5
	// and 6 are positions that never existed rather than rows that were lost.
	// Reporting them as missing alongside a range described as unaudited was the
	// contradiction this fixture exists to rule out — the same two positions
	// called lost by one pass and planted by another.
	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
		"positions above the audited bound were never allocated, so they are not missing rows, got %v", errs)
}

// TestCheck_LogBounds_MaxUint64KeyIsVerified closes the top of the key space.
//
// Pebble's iterator upper bound is exclusive, so a bound built from an
// eight-byte 0xFF run — dal.MaxUint64Bytes, and what this loop used to inline —
// is byte-identical to the key of the row at math.MaxUint64 and skips exactly
// that row. The one pass built to see planted rows could not see the one planted
// at the top: the mirror of the reserved sequence 0 hole below.
//
// The run must also COMPLETE. Widening the bound without bounding the interior
// gap scan hands the scan a sequence of 2^64-1 and it emits one event per
// missing position, which does not terminate in any useful sense. That is why
// the assertion is on a small, bounded event set and not merely on the finding.
func TestCheck_LogBounds_MaxUint64KeyIsVerified(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("maxkey"))

	for i := range 3 {
		engine.processAndCommit(createTransactionOrder("maxkey", true,
			newPosting("world", "user:alice", "USD", int64(100*(i+1))),
		))
	}

	audited, _ := highestStoredLogKey(t, engine.store)
	require.EqualValues(t, 4, audited)

	writeRawLogRows(t, engine.store, math.MaxUint64, math.MaxUint64)

	head, _ := highestStoredLogKey(t, engine.store)
	require.EqualValues(t, uint64(math.MaxUint64), head,
		"the fixture helper must see the row too, or the bound it uses has the same hole")

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.Less(t, len(errs), 10,
		"the run must stay bounded: one row near the top of the key space must not enumerate the range beneath it, got %d events", len(errs))

	unaudited := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)
	require.Len(t, unaudited, 1, "the forged row must be reported, got %v", errs)
	require.EqualValues(t, audited+1, unaudited[0].GetLogSequence())
	require.Contains(t, unaudited[0].GetMessage(), fmt.Sprintf("the store holds logs up to %d", uint64(math.MaxUint64)))
	require.Contains(t, unaudited[0].GetMessage(), "(1 logs)")

	require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP),
		"positions above the audited bound were never allocated, so they are not missing rows, got %v", errs)
}

// TestCheck_LogBounds_InjectedMaxUint64DoesNotEraseLostTail is the reason the
// two divergences are reported independently rather than chosen between.
//
// A store can be missing its audited tail AND hold a planted row at the same
// time. When one stored head drove a switch, the planted row selected the
// unaudited branch and the SEQUENCE_GAP naming the real missing tail vanished:
// one forged row would have hidden the exact restore failure this pass exists to
// catch. Both findings must survive each other.
func TestCheck_LogBounds_InjectedMaxUint64DoesNotEraseLostTail(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("both"))
	engine.processAndCommit(createTransactionOrder("both", true,
		newPosting("world", "bank", "USD", 5000),
	))
	engine.processAndCommit(createTransactionOrder("both", false,
		newPosting("bank", "user:alice", "USD", 900),
	))

	storedMax, _ := highestStoredLogKey(t, engine.store)
	require.EqualValues(t, 3, storedMax)

	// An audited tail the store no longer holds: logs 4..6 the chain accounts
	// for, each with the per-order item a real apply would have written.
	for sequence := uint64(4); sequence <= 6; sequence++ {
		appendEngineAuditEntry(t, engineAuditEntry{
			engine: engine,
			entry: &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Success{
				Success: &auditpb.AuditSuccess{MinLogSequence: sequence, MaxLogSequence: sequence},
			}},
			items: successRangeAuditItems(sequence, sequence),
		})
	}

	// ...and one row planted above everything the audit produced.
	writeRawLogRows(t, engine.store, math.MaxUint64, math.MaxUint64)

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	gaps := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)
	require.Len(t, gaps, 1, "the lost tail must still be reported, got %v", errs)
	require.EqualValues(t, 4, gaps[0].GetLogSequence())
	require.Contains(t, gaps[0].GetMessage(), "log sequences 4..6 are missing")
	require.Contains(t, gaps[0].GetMessage(), "(3 logs)")

	unaudited := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED)
	require.Len(t, unaudited, 1, "the planted row must also be reported, got %v", errs)
	require.EqualValues(t, 7, unaudited[0].GetLogSequence())
	require.Contains(t, unaudited[0].GetMessage(), "(1 logs)")
}

// TestCheck_LogBounds_SuccessRangeWithoutItemsIsNotAnOracle pins the range a
// success DECLARES against the fresh-log positions its own chain-verified items
// CARRY.
//
// Both are hash-bound, and the hash proves each was persisted as supplied — it
// does not prove they describe the same apply. Trusting the header alone made a
// chain-valid success declaring 1..4 with no items at all an oracle for four Log
// rows no order ever produced, and the whole run reported nothing.
func TestCheck_LogBounds_SuccessRangeWithoutItemsIsNotAnOracle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		items []*auditpb.AuditItem
		want  string
	}{
		{
			name:  "no items at all",
			items: nil,
			want:  "account for only 0 of those 4 positions, the first unaccounted one being 1",
		},
		{
			name: "a hole inside the declared range",
			items: []*auditpb.AuditItem{
				{OrderIndex: 0, LogSequence: 1},
				{OrderIndex: 1, LogSequence: 2},
				{OrderIndex: 2, LogSequence: 4},
			},
			want: "account for only 3 of those 4 positions, the first unaccounted one being 3",
		},
		{
			name: "a duplicate standing in for a missing position",
			items: []*auditpb.AuditItem{
				{OrderIndex: 0, LogSequence: 1},
				{OrderIndex: 1, LogSequence: 2},
				{OrderIndex: 2, LogSequence: 3},
				{OrderIndex: 3, LogSequence: 3},
			},
			want: "account for only 3 of those 4 positions, the first unaccounted one being 4",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			writeRawLogRows(t, store, 1, 4)

			gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, auditOnlyClusterID)

			entry := &auditpb.AuditEntry{
				Sequence:    1,
				Timestamp:   &commonpb.Timestamp{Data: 1700000001},
				ProposalId:  1,
				HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
				OrderCount:  uint32(len(tc.items)),
				Outcome: &auditpb.AuditEntry_Success{
					Success: &auditpb.AuditSuccess{MinLogSequence: 1, MaxLogSequence: 4},
				},
			}

			headerPayload, err := state.BuildHashedHeaderPayload(entry)
			require.NoError(t, err)

			hashSlices := make([][]byte, 0, 1+len(tc.items))
			hashSlices = append(hashSlices, headerPayload)

			for _, item := range tc.items {
				hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
			}

			_, entry.Hash = gen.Compute(nil, nil, hashSlices)

			rewriteAuditEntry(t, store, entry, tc.items)

			errs := collectCheckErrors(t, store, attributes.New())

			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH),
				"the fixture's chain must be valid, or the finding under test is not the one being pinned: %v", errs)

			incomplete := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE)
			require.Len(t, incomplete, 1,
				"a range its own items do not account for must suppress the bound, got %v", errs)
			require.Contains(t, incomplete[0].GetMessage(), tc.want)

			require.Empty(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED),
				"a suppressed bound must not also be compared, got %v", errs)
		})
	}
}

// TestCheck_LogBounds_ItemlessRangeAloneIsRejected is the reviewer's exact
// reproduction, kept as its own case: Log rows 1..4 under a chain-valid Success
// declaring 1..4 with no AuditItem rows used to return zero findings.
func TestCheck_LogBounds_ItemlessRangeAloneIsRejected(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeRawLogRows(t, store, 1, 4)
	persistItemlessSuccessAuditEntry(t, store, 1, 4)

	errs := collectCheckErrors(t, store, attributes.New())

	require.NotEmpty(t, errs, "an itemless success range must not read as a clean store")
	require.Len(t, errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE), 1,
		"got %v", errs)
}

// TestCheck_LogBounds_HeadIsKeyDerived pins the run's head to the Pebble KEY of
// the last Log row, not to that row's `sequence` field.
//
// The two are bound by convention alone — Log rows are not hash-bound — and the
// field is the one an attacker edits: it used to size the progress total and to
// align the reverse-map oracle, so a single edit moved both. The edit is itself
// a reported finding, which is exactly why nothing may be measured from it.
func TestCheck_LogBounds_HeadIsKeyDerived(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		valueSequence uint64
	}{
		// 0 is the dangerous value: it is what an empty store reports, so on the
		// last row it forged an empty history.
		{"last row claims sequence zero", 0},
		{"last row claims a later sequence", 99},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := newTestEngine(t)
			engine.processAndCommit(createLedgerOrder("head"))

			for i := range 3 {
				engine.processAndCommit(createTransactionOrder("head", true,
					newPosting("world", "user:alice", "USD", int64(100*(i+1))),
				))
			}

			head, _ := highestStoredLogKey(t, engine.store)
			require.EqualValues(t, 4, head)

			rewriteLogSequenceField(t, engine.store, head, tc.valueSequence)

			progress := collectCheckProgress(t, engine.store)
			require.NotEmpty(t, progress)

			last := progress[len(progress)-1]
			require.EqualValues(t, head, last.GetTotalLogs(),
				"the progress total must come from the key, not from the edited value field")
			require.EqualValues(t, head, last.GetLogsChecked())

			// The edit is still reported, and it is still the only finding: the
			// row is replayed under its key sequence, so no projection diverges.
			errs := collectCheckErrors(t, engine.store, engine.attrs)
			require.Len(t, errs, 1, "the edited field must be the only finding, got %v", errs)
			require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH,
				errs[0].GetErrorType())

			requireNoLogBoundFindings(t, errs)
		})
	}
}

// TestCheck_LogBounds_WideInteriorGapStaysBounded pins the interior scan's
// emission to the number of RUNS rather than the number of missing positions.
//
// Per-sequence emission is unbounded in the size of the hole, and the hole is
// chosen by whoever wrote the row above it: one forged Log row at a high key
// made the run emit that many events before it could reach any other finding.
// This fixture keeps the chain out of the way — no audit history, so the scan is
// uncapped — which is the shape that used to enumerate.
func TestCheck_LogBounds_WideInteriorGapStaysBounded(t *testing.T) {
	t.Parallel()

	const forged = uint64(1) << 32

	store := createTestStore(t)
	writeRawLogRows(t, store, 1, 2)
	writeRawLogRows(t, store, forged, forged)

	errs := collectCheckErrors(t, store, attributes.New())

	gaps := errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP)
	require.Len(t, gaps, 1,
		"a hole of %d positions must be one event, not one per position", forged-3)
	require.EqualValues(t, 3, gaps[0].GetLogSequence())
	require.Contains(t, gaps[0].GetMessage(), fmt.Sprintf("log sequences 3..%d are missing (%d logs)", forged-1, forged-3))
}
