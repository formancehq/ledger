package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

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

// auditOnlyClusterID matches the cluster id collectCheckErrors builds its
// Checker with, so a fixture persisted under it re-hashes identically when the
// full Check() walks the chain.
const auditOnlyClusterID = "test-cluster"

// newFailureAuditEntry builds the audit envelope a REJECTED proposal writes:
// one AuditEntry carrying a Failure outcome plus one AuditItem per order, each
// with LogSequence 0. That is what the FSM produces —
// writeAuditEntry(failureEntry, nil, ...) reaches
// buildAuditItems(serializedOrders, nil) — and no Log row goes with it. A
// history made only of these entries therefore has a complete hash chain over
// an empty log stream, which is the shape EN-1526 left unverified.
//
// No idempotency key is set: the frozen-outcome comparison is exercised
// separately by TestCheck_FailureOnlyHistory_ReportsTamperedIdempotencyOutcome,
// and an unkeyed entry keeps this fixture's expectation empty.
func newFailureAuditEntry(sequence uint64, orderCount int) (*auditpb.AuditEntry, []*auditpb.AuditItem) {
	entry := &auditpb.AuditEntry{
		Sequence:    sequence,
		Timestamp:   &commonpb.Timestamp{Data: 1700000000 + sequence},
		ProposalId:  sequence,
		OrderCount:  uint32(orderCount),
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
				Message: "balance too low",
				Context: map[string]string{"account": "bank"},
			},
		},
	}

	items := make([]*auditpb.AuditItem, orderCount)

	for i := range items {
		items[i] = &auditpb.AuditItem{
			OrderIndex: uint32(i),
			// LogSequence stays 0: the proposal was rejected, so no order
			// produced a log.
			SerializedOrder: (&raftcmdpb.Order{}).MarshalDeterministicVT(nil),
		}
	}

	return entry, items
}

// persistFailureOnlyHistory writes `entries` chained failure envelopes, each
// with `ordersPerEntry` items, and returns them. The chain is built forward so
// each entry consumes its predecessor's hash, exactly as the apply path does.
func persistFailureOnlyHistory(
	t *testing.T,
	store *dal.Store,
	entries int,
	ordersPerEntry int,
) ([]*auditpb.AuditEntry, [][]*auditpb.AuditItem) {
	t.Helper()

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, auditOnlyClusterID)

	var (
		lastHash    []byte
		builtEntry  = make([]*auditpb.AuditEntry, 0, entries)
		builtItems  = make([][]*auditpb.AuditItem, 0, entries)
		hashScratch []byte
	)

	for sequence := uint64(1); sequence <= uint64(entries); sequence++ {
		entry, items := newFailureAuditEntry(sequence, ordersPerEntry)

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

		builtEntry = append(builtEntry, entry)
		builtItems = append(builtItems, items)
	}

	return builtEntry, builtItems
}

// countErrorsOfType returns how many collected events carry the given type.
func countErrorsOfType(errs []*servicepb.CheckStoreError, want servicepb.CheckStoreErrorType) int {
	count := 0

	for _, e := range errs {
		if e.GetErrorType() == want {
			count++
		}
	}

	return count
}

// collectCheckProgress runs the full Check() and returns every progress event
// it emitted, in order.
func collectCheckProgress(t *testing.T, store *dal.Store) []*servicepb.CheckStoreProgress {
	t.Helper()

	checker := NewChecker(store, attributes.New(), auditOnlyClusterID, nil, logging.Testing())

	var progress []*servicepb.CheckStoreProgress

	require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if p, ok := event.GetType().(*servicepb.CheckStoreEvent_Progress); ok {
			progress = append(progress, p.Progress)
		}
	}))

	return progress
}

// TestCheck_FailureOnlyHistory_NoLogs_Clean pins the baseline EN-1526 needs:
// a store whose whole history is rejected proposals has audit entries and
// items but no logs, and the full Check() must walk it without reporting
// anything.
func TestCheck_FailureOnlyHistory_NoLogs_Clean(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		entries        int
		ordersPerEntry int
	}{
		{"single failure, single order", 1, 1},
		{"single failure, several orders", 1, 4},
		{"chained failures, several orders each", 3, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			persistFailureOnlyHistory(t, store, tc.entries, tc.ordersPerEntry)

			// The premise: no Log row exists, so the checker's idea of the
			// history head is 0 — the condition that used to short-circuit
			// Check() before verifyAuditHashChain.
			handle, err := store.NewReadHandle()
			require.NoError(t, err)

			lastSequence, err := query.ReadLastSequence(handle)
			require.NoError(t, err)
			require.NoError(t, handle.Close())
			require.Zero(t, lastSequence, "fixture must hold no logs")

			require.Empty(t, collectCheckErrors(t, store, attributes.New()),
				"a faithful failure-only history must produce no check errors")
		})
	}
}

// TestCheck_FailureOnlyHistory_ReportsTamperedAuditHash is the EN-1526
// regression proper: the tampered entry must be reported by the FULL Check(),
// not merely by verifyAuditHashChain called directly. Before the fix Check()
// returned clean here, because lastSequence == 0 short-circuited it above the
// only call site of the chain verifier.
func TestCheck_FailureOnlyHistory_ReportsTamperedAuditHash(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(entry *auditpb.AuditEntry)
	}{
		{"failure message", func(e *auditpb.AuditEntry) { e.GetFailure().Message = "you have plenty of money" }},
		{"failure reason", func(e *auditpb.AuditEntry) {
			e.GetFailure().Reason = commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND
		}},
		{"proposal id", func(e *auditpb.AuditEntry) { e.ProposalId += 100 }},
		{"stored hash", func(e *auditpb.AuditEntry) { e.Hash = []byte("forged-chain-hash") }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := createTestStore(t)
			entries, items := persistFailureOnlyHistory(t, store, 1, 2)

			require.Empty(t, collectCheckErrors(t, store, attributes.New()),
				"baseline before tampering must be clean")

			tc.mutate(entries[0])
			rewriteAuditEntry(t, store, entries[0], items[0])

			errs := collectCheckErrors(t, store, attributes.New())
			require.Equal(t, 1, countErrorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH),
				"Check() must report exactly one HASH_MISMATCH on a failure-only history, got %v", errs)
		})
	}
}

// TestCheck_FailureOnlyHistory_ReportsTamperedIdempotencyOutcome covers the
// second projection the chain walk feeds: a rejected keyed proposal freezes its
// outcome under SubIdempKeys, and the expectation is re-derived from the
// chain-verified audit entry. Over a log-less history the whole comparison used
// to be unreachable from Check().
func TestCheck_FailureOnlyHistory_ReportsTamperedIdempotencyOutcome(t *testing.T) {
	t.Parallel()

	const (
		idemKey   = "audit-only-batch-key"
		createdAt = 1700000001
	)

	// A rejected keyed proposal: one real order, its failure entry, and the
	// frozen outcome the FSM would have written alongside it.
	orders := []*raftcmdpb.Order{{}}
	proposalHash := processing.HashOrders(orders)

	buildStore := func(t *testing.T) *dal.Store {
		t.Helper()

		store := createTestStore(t)

		entry, items := newFailureAuditEntry(1, len(orders))
		entry.Timestamp = &commonpb.Timestamp{Data: createdAt}
		entry.Idempotency = &commonpb.Idempotency{Key: idemKey}
		persistAuditEntry(t, store, entry, items, auditOnlyClusterID)

		return store
	}

	faithful := &commonpb.IdempotencyKeyValue{
		CreatedAt: createdAt,
		Hash:      proposalHash,
		Failure: &commonpb.IdempotencyFailure{
			Reason:   commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
			Message:  "balance too low",
			Metadata: map[string]string{"account": "bank"},
		},
	}

	t.Run("faithful frozen outcome is clean", func(t *testing.T) {
		t.Parallel()

		store := buildStore(t)
		writeIdempotencyEntry(t, store, idemKey, faithful)

		require.Empty(t, collectCheckErrors(t, store, attributes.New()),
			"a frozen outcome matching its audit entry must not be reported")
	})

	cases := []struct {
		name   string
		mutate func(v *commonpb.IdempotencyKeyValue)
	}{
		{"failure message", func(v *commonpb.IdempotencyKeyValue) { v.Failure.Message = "you have plenty of money" }},
		{"failure reason", func(v *commonpb.IdempotencyKeyValue) {
			v.Failure.Reason = commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND
		}},
		{"proposal hash", func(v *commonpb.IdempotencyKeyValue) { v.Hash = []byte("forged-hash") }},
		// A nil failure with a log range is how a SUCCESS is frozen; the audit
		// entry says the proposal was rejected.
		{"outcome flipped to success", func(v *commonpb.IdempotencyKeyValue) {
			v.Failure = nil
			v.FirstLogSequence = 1
			v.LogCount = 1
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := buildStore(t)

			tampered := faithful.CloneVT()
			tc.mutate(tampered)
			writeIdempotencyEntry(t, store, idemKey, tampered)

			errs := collectCheckErrors(t, store, attributes.New())
			require.NotZero(t,
				countErrorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH),
				"Check() must report IDEMPOTENCY_MISMATCH on a failure-only history, got %v", errs)
		})
	}
}

// rewriteLogSequenceField rewrites the Log row at `keySequence` with its
// `sequence` field set to `valueSequence`, leaving the Pebble key alone. That
// is the divergence CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH exists to
// report: Log rows are not hash-bound, so nothing else holds the two copies of
// the sequence together.
func rewriteLogSequenceField(t *testing.T, store *dal.Store, keySequence, valueSequence uint64) {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	log, err := query.ReadLogBySequence(context.Background(), handle, keySequence)
	require.NoError(t, err)
	require.NoError(t, handle.Close())
	require.NotNil(t, log, "no log stored at sequence %d", keySequence)

	log.Sequence = valueSequence

	batch := store.OpenWriteSession()
	batch.KeyBuilder.
		PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).
		PutUint64(keySequence)
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), log))
	require.NoError(t, batch.Commit())
}

// tamperFirstAuditEntry breaks the chain on the first audit entry of a store
// built by testEngine, without touching its items.
func tamperFirstAuditEntry(t *testing.T, store *dal.Store) {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	cursor, err := query.ReadAuditEntries(context.Background(), handle, nil)
	require.NoError(t, err)

	entry, err := cursor.Next()
	require.NoError(t, err)
	require.NoError(t, cursor.Close())
	require.NoError(t, handle.Close())

	entry.ProposalId += 1000
	rewriteAuditEntry(t, store, entry, nil)
}

// TestCheck_LogSequenceFieldMismatch is the other half of EN-1526. The Log
// value's `sequence` field is what query.ReadLastSequence reports as the
// history head, and nothing binds it to the key. Editing that one field on the
// last log used to make a populated store look empty and take the removed
// early-return; the checker now reports the divergence and still verifies the
// audit chain.
func TestCheck_LogSequenceFieldMismatch(t *testing.T) {
	t.Parallel()

	// The engine writes 4 logs: one create_ledger plus three transactions.
	const lastLogSequence = 4

	buildStore := func(t *testing.T) *testEngine {
		t.Helper()

		engine := newTestEngine(t)
		engine.processAndCommit(createLedgerOrder("audit-only"))

		for i := range 3 {
			engine.processAndCommit(createTransactionOrder("audit-only", true,
				newPosting("world", "user:alice", "USD", int64(100*(i+1))),
			))
		}

		return engine
	}

	cases := []struct {
		name          string
		keySequence   uint64
		valueSequence uint64
	}{
		// 0 is the dangerous value: it is what ReadLastSequence returns for an
		// empty store, so on the LAST log it forged an empty history.
		{"last log claims sequence zero", lastLogSequence, 0},
		{"last log claims a later sequence", lastLogSequence, 99},
		{"middle log claims another sequence", 2, 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine := buildStore(t)
			require.Empty(t, collectCheckErrors(t, engine.store, engine.attrs),
				"baseline before tampering must be clean")

			rewriteLogSequenceField(t, engine.store, tc.keySequence, tc.valueSequence)

			errs := collectCheckErrors(t, engine.store, engine.attrs)
			require.Equal(t, 1,
				countErrorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH),
				"exactly one LOG_SEQUENCE_MISMATCH expected, got %v", errs)

			for _, e := range errs {
				if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH {
					require.Equal(t, tc.keySequence, e.GetLogSequence(),
						"the event must name the KEY sequence, the position every projection is keyed on")
				}
			}
		})
	}

	// The point of the fix: a forged empty history no longer buys an unverified
	// audit chain. Tamper both a log's sequence field and an audit entry, and
	// both must be reported by the same run.
	t.Run("audit chain still verified over a forged empty history", func(t *testing.T) {
		t.Parallel()

		engine := buildStore(t)
		rewriteLogSequenceField(t, engine.store, lastLogSequence, 0)
		tamperFirstAuditEntry(t, engine.store)

		errs := collectCheckErrors(t, engine.store, engine.attrs)
		require.NotZero(t,
			countErrorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH),
			"LOG_SEQUENCE_MISMATCH expected, got %v", errs)
		require.NotZero(t,
			countErrorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH),
			"HASH_MISMATCH expected in the same run, got %v", errs)
	})
}

// TestCheck_EmptyStore_EmitsSingleProgressEvent pins the progress contract
// after the early return was removed. The removed branch emitted (0, 0) itself;
// the surviving unconditional post-loop event must keep a log-less run
// distinguishable from a run that never started.
func TestCheck_EmptyStore_EmitsSingleProgressEvent(t *testing.T) {
	t.Parallel()

	t.Run("no logs at all", func(t *testing.T) {
		t.Parallel()

		progress := collectCheckProgress(t, createTestStore(t))
		require.Len(t, progress, 1, "an empty store must emit exactly one progress event")
		require.Zero(t, progress[0].GetLogsChecked())
		require.Zero(t, progress[0].GetTotalLogs())
	})

	t.Run("failure-only history", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		persistFailureOnlyHistory(t, store, 2, 2)

		progress := collectCheckProgress(t, store)
		require.Len(t, progress, 1, "a failure-only history has no logs and must emit exactly one progress event")
		require.Zero(t, progress[0].GetLogsChecked())
		require.Zero(t, progress[0].GetTotalLogs())
	})

	t.Run("populated store reports its head once at the end", func(t *testing.T) {
		t.Parallel()

		engine := newTestEngine(t)
		engine.processAndCommit(createLedgerOrder("progress"))

		for i := range 3 {
			engine.processAndCommit(createTransactionOrder("progress", true,
				newPosting("world", "user:alice", "USD", int64(100*(i+1))),
			))
		}

		checker := NewChecker(engine.store, engine.attrs, engine.clusterID, nil, logging.Testing())

		var progress []*servicepb.CheckStoreProgress

		require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
			if p, ok := event.GetType().(*servicepb.CheckStoreEvent_Progress); ok {
				progress = append(progress, p.Progress)
			}
		}))

		require.NotEmpty(t, progress)

		last := progress[len(progress)-1]
		require.EqualValues(t, 4, last.GetLogsChecked())
		require.EqualValues(t, 4, last.GetTotalLogs())

		// No duplicate tail: the post-loop emit is skipped when an in-loop
		// event already reported the same position, so the head is reported
		// exactly once. See TestCheck_HeadOnProgressBoundary_EmitsSingleProgressEvent
		// for the log count where the two emit sites actually collide.
		tail := 0

		for _, p := range progress {
			if p.GetLogsChecked() == last.GetLogsChecked() {
				tail++
			}
		}

		require.Equal(t, 1, tail, "the final position must be emitted exactly once")
	})
}

// TestCheck_HeadOnProgressBoundary_EmitsSingleProgressEvent pins the dedup on
// the one log count where the two emit sites collide: a head that is itself a
// multiple of progressInterval. The in-loop emit fires at that sequence and
// the post-loop emit reports the same position, so a consumer saw the head
// arrive twice. Dropping the in-loop `seq == lastSequence` case did not close
// this, because the collision is with the interval, not with lastSequence.
func TestCheck_HeadOnProgressBoundary_EmitsSingleProgressEvent(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	// Log rows written directly, with the audit chain declaring the matching
	// success range so no other pass has anything to report against them.
	writeRawLogRows(t, store, 1, progressInterval)
	persistSuccessAuditEntries(t, store, [][2]uint64{{1, progressInterval}})

	progress := collectCheckProgress(t, store)
	require.Len(t, progress, 1,
		"a head on the progress interval must be reported once, got %v", progress)
	require.EqualValues(t, progressInterval, progress[0].GetLogsChecked())
	require.EqualValues(t, progressInterval, progress[0].GetTotalLogs())
}
