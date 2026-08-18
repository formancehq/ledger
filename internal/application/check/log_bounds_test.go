package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
