package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestCheckVerifiesFailureOnlyHistory pins EN-1526: a store that only ever
// REJECTED proposals holds a full audit chain and zero logs, because the failure
// path writes the audit entry (machine.go:1495) and returns at :1508, before
// Merge at :1568 ever appends a log. Check() reads lastSequence out of the
// SubColdLog projection and returns early when it is zero (checker.go:165-211),
// so verifyAuditHashChain — the only audit-hash recomputation in the whole
// repository, at checker.go:328, below that return — never runs. A tampered hash
// on such a store is reported healthy.
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
}
