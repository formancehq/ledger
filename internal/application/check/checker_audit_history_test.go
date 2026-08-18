package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
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

	require.NotEmpty(t, divergences,
		"a log row whose value sequence disagrees with its key is unreachable through "+
			"AppendLogs and must be reported, not silently steer ReadLastSequence")
}
