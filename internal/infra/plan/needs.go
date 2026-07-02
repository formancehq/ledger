package plan

import (
	"github.com/formancehq/ledger/v3/internal/domain"
)

// AttributeSet is the per-attribute-code preload requirement: the set of
// canonical key bytes admission wants covered. The resolver emits Value
// (Pebble seed) or Declare (coverage-only) plans for these based on the
// CheckCache verdict.
//
// Keyed by string(canonical) — Go requires comparable map keys and
// []byte is not comparable. The string is a memory-cheap view (no copy
// semantics apply here since keys arrive already-allocated from each
// domain key's Bytes() method).
type AttributeSet struct {
	Keys map[string]struct{}
}

// Needs describes the preload requirements for a command.
//
// Attributes[attrCode] holds the per-cache preload requirement, where
// attrCode is a dal.SubAttr* byte constant. A missing entry means "no
// preload for that cache in this proposal". This shape collapses what
// used to be 13 typed key maps into a single generic dispatch keyed by
// attribute code — the same code the FSM uses to route through
// AttributePlan.attr_code.
//
// IdempotencyKeys stay separate: they do not live in the attribute
// cache (they have a dedicated IdempotencyStore), so the resolver
// pipeline treats them via its own load path and they carry no
// bloom-filter / rotation semantics.
type Needs struct {
	Attributes      map[byte]*AttributeSet
	IdempotencyKeys map[domain.IdempotencyKey]struct{}
}

// Add records `canonical` under attrCode's Keys set. Idempotent.
// Callers pass the result of the domain key's Bytes() method.
func (n *Needs) Add(attrCode byte, canonical []byte) {
	n.set(attrCode).Keys[string(canonical)] = struct{}{}
}

// Has reports whether `canonical` is in attrCode's Keys set.
// Primarily a test helper (production reads iterate via AttributeSet
// directly).
func (n *Needs) Has(attrCode byte, canonical []byte) bool {
	set, ok := n.Attributes[attrCode]
	if !ok {
		return false
	}

	_, ok = set.Keys[string(canonical)]

	return ok
}

// Count returns the number of Keys declared for attrCode (0 if the
// attrCode has no entry). Test helper.
func (n *Needs) Count(attrCode byte) int {
	set, ok := n.Attributes[attrCode]
	if !ok {
		return 0
	}

	return len(set.Keys)
}

func (n *Needs) set(attrCode byte) *AttributeSet {
	if n.Attributes == nil {
		n.Attributes = make(map[byte]*AttributeSet)
	}

	set, ok := n.Attributes[attrCode]
	if !ok {
		set = &AttributeSet{
			Keys: make(map[string]struct{}),
		}
		n.Attributes[attrCode] = set
	}

	return set
}

// TotalKeys returns the total number of keys across all attribute
// caches AND idempotency keys — every key the preload pipeline handles.
func (n *Needs) TotalKeys() int {
	return n.AttributeKeysCount() + len(n.IdempotencyKeys)
}

// AttributeKeysCount returns the total number of cache-attribute keys.
// Idempotency keys are excluded: a proposal with idempotency keys only
// does not need the cache-epoch revalidation that the slow path
// performs. The runner uses this count to gate runWithoutPreload, so
// idempotency-only proposals (maintenance, signing, chapter schedule)
// take the fast path and avoid spurious ErrStaleProposal on
// cluster-config resets.
func (n *Needs) AttributeKeysCount() int {
	total := 0
	for _, set := range n.Attributes {
		total += len(set.Keys)
	}

	return total
}

// Merge unions every key set from src into dst. Used by admission to
// roll per-order Needs into a single proposal-wide Needs while keeping
// the per-order slice available for coverage_bits computation.
func (n *Needs) Merge(src *Needs) {
	for attrCode, set := range src.Attributes {
		dst := n.set(attrCode)
		for k := range set.Keys {
			dst.Keys[k] = struct{}{}
		}
	}

	for k := range src.IdempotencyKeys {
		n.IdempotencyKeys[k] = struct{}{}
	}
}

// NewNeeds creates a Needs with initialized maps. Per-attribute sets
// are created lazily on first Add so an empty Needs stays cheap.
func NewNeeds() *Needs {
	return &Needs{
		Attributes:      make(map[byte]*AttributeSet),
		IdempotencyKeys: make(map[domain.IdempotencyKey]struct{}),
	}
}
