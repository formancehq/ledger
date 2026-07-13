package plan

import (
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// planLookupKey indexes the proposal's AttributeCoverage slice. Multiple plans
// can share the same U128 (e.g. a LedgerKey's "gaming" canonical underpins
// both the Ledgers attribute and the Boundaries attribute — same canonical
// bytes, same U128) so the lookup must include the attribute code to
// disambiguate them.
type planLookupKey struct {
	id       attributes.U128
	attrCode byte
}

// bitsForNeeds returns the packed coverage_bits bitset over plans for
// one set of Coverage. Used by Run internally to flow per-WriteOperation
// needs onto each operation's coverage field at marshal time.
//
// Returns nil when plans is empty (no coverage to flag) or needs is nil.
//
// When assigning coverage for many WriteOperations against the same
// plans slice (the admission batch case), prefer bitsForNeedsWithIndex
// after a single buildPlanIndex call — applyBits does exactly that to
// amortize the index build across the whole batch.
func bitsForNeeds(needs *Coverage, plans []*raftcmdpb.AttributeCoverage) []byte {
	if needs == nil || len(plans) == 0 {
		return nil
	}

	return bitsForNeedsWithIndex(needs, len(plans), buildPlanIndex(plans))
}

// bitsForNeedsWithIndex is the inner loop of bitsForNeeds: same output,
// but the caller is responsible for building the index once and passing
// it in. planCount is the number of AttributeCoverage entries the index was
// built over — used to size the returned bitset, since the index may
// have fewer entries than the slice (idempotency-key plans are skipped).
//
// Returns nil when needs is nil; callers that have already filtered
// empty-plans cases keep that responsibility (applyBits does).
func bitsForNeedsWithIndex(needs *Coverage, planCount int, index map[planLookupKey]uint32) []byte {
	if needs == nil {
		return nil
	}

	bits := make([]byte, (planCount+7)/8)
	setIDInBitset(bits, index, needs)

	return bits
}

// buildPlanIndex maps each AttributeCoverage (keyed by canonical U128 + its
// attr_code) to its position in the proposal's plans slice. Idempotency-
// key plans (AttributeID == nil) are skipped: they're not coverage-checked.
func buildPlanIndex(plans []*raftcmdpb.AttributeCoverage) map[planLookupKey]uint32 {
	index := make(map[planLookupKey]uint32, len(plans))

	for i, plan := range plans {
		attrID := plan.GetId()
		if attrID == nil {
			continue
		}

		index[planLookupKey{id: attributes.U128FromBytes(attrID.GetId()), attrCode: planAttrCode(plan)}] = uint32(i)
	}

	return index
}

// planAttrCode returns the attribute code (dal.SubAttrXxx) of the given
// plan. attr_code lives on the AttributeCoverage itself, so the kind is
// read uniformly regardless of intent — no oneof dispatch needed.
func planAttrCode(plan *raftcmdpb.AttributeCoverage) byte {
	return byte(plan.GetAttrCode())
}

// setIDInBitset walks every (attrCode, id) pair in needs and sets the
// matching bit in bits if the pair maps to an AttributeCoverage index.
// Since Coverage stores the U128 id directly (pre-computed at Add
// time), no re-hashing or bytes/string conversion happens here — this
// is the hot path that runs once per WriteOperation on every marshal
// (twice on the rare rebuild). Keys outside the map (idempotency
// tracker, references whose preload was skipped) are silently dropped.
func setIDInBitset(bits []byte, indexByPlan map[planLookupKey]uint32, needs *Coverage) {
	for attrCode, byID := range needs.Attributes {
		for id := range byID {
			idx, ok := indexByPlan[planLookupKey{id: id, attrCode: attrCode}]
			if !ok {
				continue
			}

			bits[idx/8] |= 1 << (idx % 8)
		}
	}
}
