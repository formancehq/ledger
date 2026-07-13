package plan

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
)

// CoverageEntry is the stored value for one attribute-cache key: the
// canonical bytes (kept for Pebble Get in the resolver) alongside the
// XXH3-64 tag pre-computed at Add time. Storing the tag here means
// resolveCoverage doesn't have to rehash canonical bytes to compute
// the AttributeID.Tag — MakeKey already returned it, and we were
// discarding it under the old shape.
type CoverageEntry struct {
	Canonical []byte
	Tag       uint64
}

// Coverage describes the preload / coverage requirements for a command.
//
// Attributes[attrCode][id] = (canonical bytes, tag). `id` is the U128
// hash of the canonical bytes (attributes.MakeKey) — pre-computed at
// Add time so the downstream pipeline (parallel resolver,
// coverage_bits) never has to rehash or shuttle the bytes through a
// string round-trip. `Tag` is the XXH3-64 collision detector; the FSM
// uses it to reject preload seeds that don't match the cache entry's
// tag on U128 collisions.
//
// IdempotencyKeys is lazily allocated on first AddIdempotencyKey — most
// orders (system scoped, index management, chapter operations) carry
// none, so paying an empty-map header per order is wasteful.
//
// The map-of-maps shape collapses what used to be 13 typed key fields
// into a single generic dispatch keyed by attribute code — the same code
// the FSM uses to route through AttributeCoverage.attr_code.
type Coverage struct {
	Attributes      map[byte]map[attributes.U128]CoverageEntry
	IdempotencyKeys map[domain.IdempotencyKey]struct{}

	// collision holds the first XXH3-128 collision detected by Add/Merge
	// (two distinct canonical keys sharing a 128-bit id). It is surfaced
	// as a hard error at the Build boundary via Err() so a dropped preload
	// key can never silently reach apply. nil in the overwhelmingly common
	// case (probability of a genuine collision is ~2^-128).
	collision error
}

// Add records `canonical` under attrCode's key set. Idempotent — a
// repeat Add for the same key is a no-op (the canonical bytes are
// retained from the first call). Callers pass the result of the domain
// key's Bytes() method; Coverage takes ownership of the slice, callers
// MUST NOT mutate it afterwards.
func (c *Coverage) Add(attrCode byte, canonical []byte) {
	if c.Attributes == nil {
		c.Attributes = make(map[byte]map[attributes.U128]CoverageEntry)
	}

	m, ok := c.Attributes[attrCode]
	if !ok {
		m = make(map[attributes.U128]CoverageEntry)
		c.Attributes[attrCode] = m
	}

	id, tag := attributes.MakeKey(canonical)

	if existing, exists := m[id]; !exists {
		m[id] = CoverageEntry{Canonical: canonical, Tag: tag}
	} else if existing.Tag != tag {
		// Same XXH3-128 id but a different XXH3-64 tag means two distinct
		// canonical keys genuinely collided on the 128-bit id (~2^-128).
		// The map can only hold one entry per id, so the second key would
		// be silently dropped and the order reaching apply without its
		// Pebble seed — a silent cache miss instead of the loud collision
		// the tag exists to surface (attributes.KeyStore.Get/Put rejects
		// same-id/different-tag as ErrCollisionDetected). Keep the first
		// entry but fail loudly: this is impossible by design (invariant #7).
		assert.Unreachable("coverage: XXH3-128 collision between distinct canonical keys", map[string]any{
			"attrCode":    attrCode,
			"existingTag": existing.Tag,
			"newTag":      tag,
		})
		// assert.Unreachable is a no-op in production builds, so also record
		// a clean, returnable error (first collision wins). Build checks it
		// via Err() and fails the proposal instead of dropping the key —
		// mirroring the loud attributes.KeyStore ErrCollisionDetected path.
		if c.collision == nil {
			c.collision = &attributes.ErrCollisionDetected{Bytes: canonical, OriginalTag: existing.Tag, NewTag: tag}
		}
	}
}

// AddIdempotencyKey records a batch idempotency key. Lazily allocates
// the underlying map on first call — most proposals carry none.
func (c *Coverage) AddIdempotencyKey(key string) {
	if c.IdempotencyKeys == nil {
		c.IdempotencyKeys = make(map[domain.IdempotencyKey]struct{})
	}

	c.IdempotencyKeys[domain.IdempotencyKey{Key: key}] = struct{}{}
}

// Has reports whether `canonical` is in attrCode's key set.
// Primarily a test helper (production reads iterate the map directly).
func (c *Coverage) Has(attrCode byte, canonical []byte) bool {
	m, ok := c.Attributes[attrCode]
	if !ok {
		return false
	}

	id, _ := attributes.MakeKey(canonical)

	_, ok = m[id]

	return ok
}

// Count returns the number of keys declared for attrCode (0 if the
// attrCode has no entry). Test helper.
func (c *Coverage) Count(attrCode byte) int {
	return len(c.Attributes[attrCode])
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
// Merging from a nil Coverage is a no-op.
func (c *Coverage) Merge(src *Coverage) {
	if src == nil {
		return
	}

	for attrCode, srcMap := range src.Attributes {
		if len(srcMap) == 0 {
			continue
		}

		if c.Attributes == nil {
			c.Attributes = make(map[byte]map[attributes.U128]CoverageEntry, len(src.Attributes))
		}

		dst, ok := c.Attributes[attrCode]
		if !ok {
			dst = make(map[attributes.U128]CoverageEntry, len(srcMap))
			c.Attributes[attrCode] = dst
		}

		for id, entry := range srcMap {
			if existing, exists := dst[id]; !exists {
				dst[id] = entry
			} else if existing.Tag != entry.Tag {
				// Genuine XXH3-128 collision across the two Coverages being
				// merged (see Add): the second key would be silently dropped.
				// Keep the first and fail loudly — impossible by design
				// (invariant #7).
				assert.Unreachable("coverage: XXH3-128 collision on Merge between distinct canonical keys", map[string]any{
					"attrCode":    attrCode,
					"existingTag": existing.Tag,
					"newTag":      entry.Tag,
				})
				if c.collision == nil {
					c.collision = &attributes.ErrCollisionDetected{Bytes: entry.Canonical, OriginalTag: existing.Tag, NewTag: entry.Tag}
				}
			}
		}
	}

	// Propagate a collision recorded on the source (e.g. from a per-order
	// Add) into the destination, so the Build-boundary Err() check sees it
	// once per-order Coverages are merged into the proposal aggregate.
	if c.collision == nil {
		c.collision = src.collision
	}

	if len(src.IdempotencyKeys) == 0 {
		return
	}

	if c.IdempotencyKeys == nil {
		c.IdempotencyKeys = make(map[domain.IdempotencyKey]struct{}, len(src.IdempotencyKeys))
	}

	for k := range src.IdempotencyKeys {
		c.IdempotencyKeys[k] = struct{}{}
	}
}

// Err returns the first XXH3-128 collision recorded during Add/Merge, or
// nil. A non-nil result means two distinct canonical keys collided on the
// same 128-bit id (~2^-128); the Build boundary turns it into a hard
// proposal failure rather than a silent preload gap. See Add.
func (c *Coverage) Err() error {
	return c.collision
}

// NewCoverage returns an empty Coverage. Maps are allocated lazily on the
// first Add / AddIdempotencyKey — a Coverage that ends up carrying
// nothing (many system-scoped orders) never touches the allocator.
func NewCoverage() *Coverage {
	return &Coverage{}
}
