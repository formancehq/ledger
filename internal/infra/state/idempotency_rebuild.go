package state

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// IdempotencyValueFromAudit re-derives the frozen idempotency value a keyed
// proposal wrote, from its (chain-verified) audit entry and items. ok is false
// when the proposal froze nothing under its key: an all-replay success (no log
// produced), a non-freezable (retryable/internal) failure, or an
// IDEMPOTENCY_KEY_CONFLICT (a rejection of a reuse, which never overwrites the
// existing outcome) — none of these write SubIdempKeys. The proposal hash is
// recomputed from the audit orders, reusing the FSM's hashing so it is
// byte-identical to what was frozen at apply time.
//
// Shared by the integrity checker (which compares it against the stored
// projection) and the backup restore path (which persists it via
// SaveIdempotencyKey), so the two never diverge.
func IdempotencyValueFromAudit(entry *auditpb.AuditEntry, items []*auditpb.AuditItem) (*commonpb.IdempotencyKeyValue, bool) {
	switch out := entry.GetOutcome().(type) {
	case *auditpb.AuditEntry_Failure:
		reason := out.Failure.GetReason()

		// A conflict (reused key + different body) freezes nothing: the FSM's gate
		// only produces it when a LIVE prior outcome exists, and
		// recordIdempotencyFailure never overwrites that prior. So it is never a
		// stored outcome — skip it. The hash-chain-bound audit reason already
		// records whether a reuse hit a live prior (conflict) or executed fresh
		// (a normal reason), so no expiry re-derivation is needed to tell them
		// apart.
		if reason == commonpb.ErrorReason_ERROR_REASON_IDEMPOTENCY_KEY_CONFLICT {
			return nil, false
		}

		if !domain.IsFreezableFailure(domain.KindForReason(reason)) {
			return nil, false
		}

		return &commonpb.IdempotencyKeyValue{
			Hash:      recomputeProposalHash(items),
			CreatedAt: entry.GetTimestamp().GetData(),
			Failure: &commonpb.IdempotencyFailure{
				Reason:   reason,
				Message:  out.Failure.GetMessage(),
				Metadata: out.Failure.GetContext(),
			},
		}, true

	case *auditpb.AuditEntry_Success:
		maxSeq := out.Success.GetMaxLogSequence()
		if maxSeq == 0 {
			return nil, false
		}

		minSeq := out.Success.GetMinLogSequence()

		return &commonpb.IdempotencyKeyValue{
			Hash:             recomputeProposalHash(items),
			FirstLogSequence: minSeq,
			LogCount:         uint32(maxSeq - minSeq + 1),
			CreatedAt:        entry.GetTimestamp().GetData(),
		}, true

	default:
		return nil, false
	}
}

// recomputeProposalHash re-derives a proposal's idempotency hash from its
// persisted audit orders, reusing the FSM's hashing so the result is
// byte-identical to what was frozen. The orders round-trip from the chain-bound
// serialized_order bytes; a corrupt order would already have broken the audit
// chain during verification, so a nil here only forces a loud hash mismatch.
func recomputeProposalHash(items []*auditpb.AuditItem) []byte {
	orders := make([]*raftcmdpb.Order, 0, len(items))

	for _, item := range items {
		order := &raftcmdpb.Order{}
		if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			return nil
		}

		orders = append(orders, order)
	}

	return processing.HashOrders(orders)
}
