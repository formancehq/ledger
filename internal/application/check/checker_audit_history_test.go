package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestCheckVerifiesFailureOnlyHistory pins EN-1526: a store that only ever
// REJECTED proposals holds a full audit chain and zero logs, because the failure
// path writes the audit entry (machine.go:1495) and returns at :1508, before
// Merge at :1568 ever appends a log.
//
// Check() used to read lastSequence out of the SubColdLog projection and return
// early when it was zero, above verifyAuditHashChain — the only audit-hash
// recomputation in the repository. A tampered hash on such a store was reported
// healthy. This test pins that the chain is verified regardless of the log count.
//
// The fixture writes ONE failure entry with a valid keyed hash, then rewrites it
// with a corrupted hash WITHOUT re-hashing, which is what an adversary that does
// not know the ClusterID-derived key would produce.
func TestCheckVerifiesFailureOnlyHistory(t *testing.T) {
	t.Parallel()

	const clusterID = "test-cluster"

	store := createTestStore(t)
	attrs := attributes.New()

	orders := []*raftcmdpb.Order{{}}
	serialized := orders[0].MarshalDeterministicVT(nil)

	entry := &auditpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &commonpb.Timestamp{Data: 1700000001},
		ProposalId:  7,
		OrderCount:  1,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
				Message: "balance too low",
			},
		},
	}
	// One item per order, LogSequence deliberately left at 0 — that is exactly
	// what buildAuditItems produces for a failure (internal/infra/state/audit.go:79-103).
	items := []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized}}

	persistAuditEntry(t, store, entry, items, clusterID)

	require.Empty(t, collectCheckErrors(t, store, attrs),
		"an untampered failure-only store must be reported clean: deleting the "+
			"lastSequence == 0 gate must not make any pass fire spuriously")

	// Tamper: flip the stored hash and rewrite without recomputing.
	entry.Hash = append([]byte(nil), entry.GetHash()...)
	entry.Hash[0] ^= 0xFF
	rewriteAuditEntry(t, store, entry, items)

	errors := collectCheckErrors(t, store, attrs)

	var hashMismatches []*servicepb.CheckStoreError

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH {
			hashMismatches = append(hashMismatches, e)
		}
	}

	require.Len(t, hashMismatches, 1,
		"a failure-only store has zero logs, so the audit chain must still be verified: "+
			"gating verification on the SubColdLog projection is the EN-1526 defect")
	require.Contains(t, hashMismatches[0].GetMessage(), "audit hash chain broken at sequence 1")
}

// TestCheck_ZeroLogPendingCleanupDoesNotSuppressOrphans pins the
// pendingCleanupLedgers guard in Check(): a persisted pending-cleanup marker is
// an unverified projection. On a zero-log store no DeleteLedger can be audited,
// so a marker cannot legitimately exist and must not suppress the stale-index
// finding it would otherwise mask. Both production writers of the marker require
// a log (state.write_set's DeleteLedger apply path, and backup rebuild's
// log-driven replay), so on a healthy store the empty set and the persisted set
// are identical.
func TestCheck_ZeroLogPendingCleanupDoesNotSuppressOrphans(t *testing.T) {
	t.Parallel()

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	key := domain.IndexKey{LedgerName: "doomed", Canonical: indexes.Canonical(id)}

	store := createTestStore(t)
	attrs := attributes.New()

	batch := store.OpenWriteSession()
	_, err := attrs.Index.Set(batch, key.Bytes(), &commonpb.Index{Id: id, Ledger: "doomed"})
	require.NoError(t, err)
	require.NoError(t, state.SavePendingLedgerCleanup(batch, "doomed", 9))
	require.NoError(t, batch.Commit())

	errs := collectCheckErrors(t, store, attrs)
	require.Len(t, errs, 1)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH, errs[0].GetErrorType())
	require.Equal(t, "doomed", errs[0].GetLedger())
}

// TestCheckDetectsLogKeyValueSequenceDivergence pins the one-field bypass from
// EN-1526. A Log row's key carries the sequence and so does its value, but the
// value is not hash-bound and nothing compared the two — so rewriting the value's
// `sequence` field alone was invisible while still steering ReadLastSequence,
// which the deleted early-return gate consumed. The row is impossible by
// contract: AppendLogs (internal/infra/state/batch.go:16-30) derives the key FROM
// log.GetSequence(), so the two cannot disagree unless the row was rewritten
// outside the FSM. Invariant #7: report it loudly.
func TestCheckDetectsLogKeyValueSequenceDivergence(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("ledger"))

	// Rewrite the value at key sequence 1 so its `sequence` field says 0, leaving
	// the key untouched. This is the minimal forgery the old gate accepted.
	handle, err := engine.store.NewReadHandle()
	require.NoError(t, err)

	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(1).Build()

	value, closer, err := handle.Get(key)
	require.NoError(t, err)

	stored := &commonpb.Log{}
	require.NoError(t, stored.UnmarshalVT(value))
	require.NoError(t, closer.Close())
	require.NoError(t, handle.Close())

	stored.Sequence = 0

	batch := engine.store.OpenWriteSession()
	require.NoError(t, batch.SetProto(key, stored))
	require.NoError(t, batch.Commit())

	errors := collectCheckErrors(t, engine.store, engine.attrs)

	var divergences []*servicepb.CheckStoreError

	for _, e := range errors {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP &&
			e.GetLogSequence() == 1 {
			divergences = append(divergences, e)
		}
	}

	// SEQUENCE_GAP has three distinct producers, so the type and sequence filter
	// above is not enough on its own: the count and the message are what
	// distinguish the key/value disagreement from a genuine hole in the stream.
	require.Len(t, divergences, 1,
		"a log row whose value sequence disagrees with its key is unreachable through "+
			"AppendLogs and must be reported exactly once, not silently steer "+
			"ReadLastSequence")
	require.Contains(t, divergences[0].GetMessage(), "carries value sequence 0")
	require.Contains(t, divergences[0].GetMessage(), "written outside AppendLogs")
}

// TestCheckVerifiesFrozenIdempotencyOnFailureOnlyStore pins the most
// client-visible half of the EN-1526 defect. A rejected proposal freezes its
// outcome under the idempotency key, and every retry inside the TTL window reads
// that frozen row back as a definitive business error — so a tampered row serves
// a forged error to the client without ever touching the audit chain.
//
// compareIdempotencyOutcomes has exactly one call site, and it sits inside
// verifyAuditHashChain (see the call in verifyAuditHashChain's tail, and the
// single caller of verifyAuditHashChain in Check). Because the deleted
// `lastSequence == 0` gate returned above that call, a failure-only store's
// frozen rows were verified NOWHERE: the failure path writes the audit entry and
// returns before any log is appended, so lastSequence stays 0 and the whole
// idempotency pass was skipped along with the chain walk.
//
// The audit entry is left untouched here on purpose. Only the persisted
// SubIdempKeys value is tampered, so the hash chain stays valid and the single
// finding proves the idempotency pass ran rather than a chain mismatch standing
// in for it.
func TestCheckVerifiesFrozenIdempotencyOnFailureOnlyStore(t *testing.T) {
	t.Parallel()

	const (
		clusterID = "test-cluster"
		idemKey   = "failure-only-retry-key"
		createdAt = 1700000001
	)

	store := createTestStore(t)
	attrs := attributes.New()

	orders := []*raftcmdpb.Order{{}}
	serialized := orders[0].MarshalDeterministicVT(nil)
	proposalHash := processing.HashOrders(orders)

	// A rejected proposal that froze its outcome: the audit failure entry and
	// nothing else. No log is appended, which is what makes lastSequence 0.
	entry := &auditpb.AuditEntry{
		Sequence:    1,
		Timestamp:   &commonpb.Timestamp{Data: createdAt},
		ProposalId:  7,
		OrderCount:  1,
		HashVersion: uint32(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3),
		Idempotency: &commonpb.Idempotency{Key: idemKey},
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{
				Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
				Message: "balance too low",
			},
		},
	}
	items := []*auditpb.AuditItem{{OrderIndex: 0, SerializedOrder: serialized}}

	persistAuditEntry(t, store, entry, items, clusterID)

	// The frozen row a retry would read back, faithful to the audit entry. Its
	// created_at equals the only verified entry's timestamp, so it sits exactly
	// at the idempotency report floor and is in scope for the pass.
	faithful := &commonpb.IdempotencyKeyValue{
		CreatedAt: createdAt,
		Hash:      proposalHash,
		Failure: &commonpb.IdempotencyFailure{
			Reason:  commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
			Message: "balance too low",
		},
	}

	writeIdempotencyEntry(t, store, idemKey, faithful)

	require.Empty(t, collectCheckErrors(t, store, attrs),
		"a failure-only store whose frozen outcome matches its audit entry must be "+
			"reported clean: verifying the idempotency pass on a zero-log store must "+
			"not fire spuriously")

	// Tamper only the frozen message: the audit chain stays intact, so nothing
	// but the idempotency pass can catch this.
	tampered := faithful.CloneVT()
	tampered.Failure.Message = "you have plenty of money"
	writeIdempotencyEntry(t, store, idemKey, tampered)

	var mismatches []*servicepb.CheckStoreError

	for _, e := range collectCheckErrors(t, store, attrs) {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH {
			mismatches = append(mismatches, e)
		}
	}

	require.Len(t, mismatches, 1,
		"the frozen outcome a retry reads back is only ever verified from inside "+
			"verifyAuditHashChain, so gating that walk on the log projection left a "+
			"failure-only store's forged business error undetected (EN-1526)")
}

// TestCheckJudgesProjectionsOnAWipedLogStream covers the shape the deleted
// `lastSequence == 0` gate actually unlocked: a store that lost its whole log
// stream but kept every projection row.
//
// That gate returned above the entire replay-and-compare phase, so on a zero-log
// store the per-ledger projection family was reachable by nothing. Every other
// zero-log fixture in this package pairs the empty log stream with
// empty-or-near-empty projections -- a failure-only chain, a frozen idempotency
// row, one index row, two hand-built audit entries -- and the pre-existing
// EmptyAuditWiring pair is a bare store plus one row. The FIELD shape is the
// opposite one, and it had no fixture at all.
//
// This is also the residual-exposure case rather than a theoretical one.
// Restoring the whole gate is already well pinned, and so is an early return
// before the compare phase. But gating only the per-ledger compares --
// `if lastSequence > 0 { compareVolumes; compareMetadata; compareTransactions }`,
// or the same around compareLedgerPresence and compareBoundaries -- suppresses
// every finding below and ships fully green. The `if lastSequence > 0` idiom is
// one screen away in Check(), around pendingCleanupLedgers, so a future author
// re-applying it there is the realistic path back to base behaviour. The input is
// untrusted: ValidateRestore runs Check() over a staged FOREIGN backup.
//
// Asserted per type via errorsOfType rather than as a total count: the exact
// cardinality shifts with unrelated passes, while "the projection family spoke at
// all" is the property under test.
func TestCheckJudgesProjectionsOnAWipedLogStream(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder("ledger"))
	engine.processAndCommit(createTransactionOrder("ledger", true,
		newPosting("world", "alice", "USD", 100)))

	require.Empty(t, collectCheckErrors(t, engine.store, engine.attrs),
		"the control must be clean, or the findings below cannot be attributed to "+
			"the wiped log stream")

	// Delete the log rows and nothing else. Every projection -- LedgerInfo,
	// volumes, transaction states, boundaries -- stays exactly as the FSM wrote
	// it, so the audit-derived expectation collapses to empty while the stored
	// state still describes a healthy ledger.
	batch := engine.store.OpenWriteSession()
	require.NoError(t, batch.DeleteRange(
		[]byte{dal.ZoneCold, dal.SubColdLog},
		[]byte{dal.ZoneCold, dal.SubColdLog + 1},
		nil))
	require.NoError(t, batch.Commit())

	errs := collectCheckErrors(t, engine.store, engine.attrs)

	require.NotEmpty(t,
		errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNAUDITED_LEDGER),
		"a stored LedgerInfo with no CreateLedger left in the replayed history must "+
			"be reported: this is the pass the deleted zero-log gate returned above")
	require.NotEmpty(t,
		errorsOfType(errs, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH),
		"stored volumes with an empty replayed expectation must be reported: gating "+
			"only compareVolumes on lastSequence ships green without this assertion")
}
