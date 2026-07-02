package plan

import (
	"github.com/formancehq/ledger/v3/internal/domain"
)

// Coverage describes the preload / coverage requirements for a command.
//
// Attributes[attrCode] is the set of canonical key bytes admission wants
// the FSM apply path to be authorized to access (read or delete) under
// dal.SubAttrXxx = attrCode. The resolver may attach a seed value to an
// entry when admission's Pebble scan resolved one (CacheMiss + Pebble-hit);
// otherwise the entry ships as coverage-only.
//
// IdempotencyKeys stay separate: they do not live in the attribute cache
// (they have a dedicated IdempotencyStore), so the resolver treats them
// via its own load path and they carry no bloom-filter / rotation
// semantics.
//
// The map-of-maps shape collapses what used to be 13 typed key fields
// into a single generic dispatch keyed by attribute code — the same code
// the FSM uses to route through AttributeCoverage.attr_code.
type Coverage struct {
	Attributes      map[byte]map[string]struct{}
	IdempotencyKeys map[domain.IdempotencyKey]struct{}
}

// Add records `canonical` under attrCode's key set. Idempotent.
// Callers pass the result of the domain key's Bytes() method.
func (c *Coverage) Add(attrCode byte, canonical []byte) {
	c.set(attrCode)[string(canonical)] = struct{}{}
}

// Has reports whether `canonical` is in attrCode's key set.
// Primarily a test helper (production reads iterate the map directly).
func (c *Coverage) Has(attrCode byte, canonical []byte) bool {
	set, ok := c.Attributes[attrCode]
	if !ok {
		return false
	}

	_, ok = set[string(canonical)]

	return ok
}

// Count returns the number of keys declared for attrCode (0 if the
// attrCode has no entry). Test helper.
func (c *Coverage) Count(attrCode byte) int {
	return len(c.Attributes[attrCode])
}

func (c *Coverage) set(attrCode byte) map[string]struct{} {
	if c.Attributes == nil {
		c.Attributes = make(map[byte]map[string]struct{})
	}

	set, ok := c.Attributes[attrCode]
	if !ok {
		set = make(map[string]struct{})
		c.Attributes[attrCode] = set
	}

	return set
}

// TotalKeys returns the total number of keys across all attribute
// caches AND idempotency keys — every key the preload pipeline handles.
func (c *Coverage) TotalKeys() int {
	return c.AttributeKeysCount() + len(c.IdempotencyKeys)
}

// AttributeKeysCount returns the total number of cache-attribute keys.
// Idempotency keys are excluded: a proposal with idempotency keys only
// does not need the cache-epoch revalidation that the slow path
// performs. The runner uses this count to gate runWithoutPreload, so
// idempotency-only proposals (maintenance, signing, chapter schedule)
// take the fast path and avoid spurious ErrStaleProposal on
// cluster-config resets.
func (c *Coverage) AttributeKeysCount() int {
	total := 0
	for _, set := range c.Attributes {
		total += len(set)
	}

	return total
}

// Merge unions every key set from src into dst. Used by admission to
// roll per-order Coverage into a single proposal-wide Coverage while
// keeping the per-order slice available for coverage_bits computation.
func (c *Coverage) Merge(src *Coverage) {
	for attrCode, set := range src.Attributes {
		dst := c.set(attrCode)
		for k := range set {
			dst[k] = struct{}{}
		}
	}

	for k := range src.IdempotencyKeys {
		c.IdempotencyKeys[k] = struct{}{}
	}
}

// NewCoverage creates a Coverage with initialized maps. Per-attribute
// sets are created lazily on first Add so an empty Coverage stays cheap.
func NewCoverage() *Coverage {
	return &Coverage{
		Attributes:      make(map[byte]map[string]struct{}),
		IdempotencyKeys: make(map[domain.IdempotencyKey]struct{}),
	}
}
