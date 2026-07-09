package plan

import (
	"encoding/hex"
	"fmt"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// resolveParallelism is the maximum number of concurrent Pebble Gets within
// a single attribute type resolution. The workload is I/O bound (Ceph RBD
// reads at ~1ms per miss), so moderate parallelism saturates the storage
// without overwhelming Pebble's iterator pool.
const resolveParallelism = 16

// resolveResult holds the output of a resolve call: a flat list of
// AttributeCoverage entries plus the tracker keys. Each entry is either
// coverage-only (value = nil) or a seed (value = Pebble-loaded payload).
//
// The FSM's Preload path routes seed entries through MirrorPreload
// (gen0+gen1 write with gen1-wins semantics). Coverage-only entries are
// pure declarations: they contribute to coverage_bits (invariant #9) so
// the scope admits their key in the FSM apply path, but they do NOT
// mutate the cache — AttributeCache.Get's gen0→gen1 fallback and
// AttributeCache.Del's lazy gen1→gen0 tombstone fabrication cover the
// read and delete cases without a preemptive promote pass.
//
// Idempotency keys live on the parallel idempotencyKeys slice — they are
// NOT cache attributes (the FSM applies them to a dedicated store) so
// they bypass the AttributeCoverage list and ship on
// ExecutionPlan.idempotency_keys.
type resolveResult struct {
	attributes      []*raftcmdpb.AttributeCoverage
	idempotencyKeys []*raftcmdpb.ReloadIdempotencyKey
	tracker         []attributes.U128
}

// coverageEntry returns a coverage-only AttributeCoverage — no seed.
// The entry is only a declaration for coverage_bits (invariant #9);
// the FSM's Preload skips it (no cache mutation), and the handler's
// Get / Del rely on AttributeCache's built-in gen0→gen1 fallback and
// lazy tombstone fabrication.
func coverageEntry(id attributes.U128, attrCode byte, tag uint64) *raftcmdpb.AttributeCoverage {
	return &raftcmdpb.AttributeCoverage{
		Id:       &raftcmdpb.AttributeID{Id: id[:], Tag: tag},
		AttrCode: uint32(attrCode),
	}
}

// seedEntry returns an AttributeCoverage carrying a Pebble-loaded seed —
// the FSM's Preload path calls MirrorPreload to write the value into both
// generations. attr_code lives on the envelope so the FSM apply path
// routes the typed unmarshal without inspecting the value payload itself.
func seedEntry(attrID *raftcmdpb.AttributeID, attrCode byte, value *raftcmdpb.AttributeValue) *raftcmdpb.AttributeCoverage {
	return &raftcmdpb.AttributeCoverage{
		Id:       attrID,
		AttrCode: uint32(attrCode),
		Value:    value,
	}
}

// resolveCoverage resolves one attribute cache for the plan pipeline.
// Keys arrive already hashed: the map key is the pre-computed U128
// (attributes.MakeKey), the map value is the canonical bytes. No
// string↔bytes round trip, no rehash on iteration — the (id, canonical)
// pair travels intact from admission's Add through to the FSM's wire
// payload.
//
// For each key, resolveCoverage emits ONE AttributeCoverage entry based
// on admission's CheckCache verdict:
//
//   - CacheUnreachable → ErrCacheHorizonExceeded (admission rejection).
//   - CacheHit → coverage-only (value nil); AttributeCache.Get's gen0→gen1
//     fallback surfaces the entry on read, and AttributeCache.Del's lazy
//     promote fabricates the gen0 tombstone if the handler deletes.
//   - CacheMiss + bloom/Pebble-absent → coverage-only.
//   - CacheMiss + Pebble-load-hit → seeded (value = the loaded payload;
//     MirrorPreload writes gen0+gen1).
//
// FSM-side race protection is handled at Preload for seeds only (via
// MirrorPreload's gen1-wins), and by the coverage_bits gate (invariant
// #9) for reads. Keys a concurrent-write race populated are visible to
// the handler via the gen0→gen1 fallback; keys nothing populated see
// ErrNotFound and produce the expected NotFound business outcome.
// Bounded by CacheUnreachable (2+ rotations rejected at admission), no
// data can be lost between admission and apply.
//
// Keys are resolved with bounded parallelism to amortize I/O latency.
func resolveCoverage[T interface {
	MarshalVT() ([]byte, error)
}](
	keys map[attributes.U128][]byte,
	nextIndex, boundary, cacheEpoch uint64,
	attrCache *cache.AttributeCache[T],
	loader *preload.AttributeLoader[T],
	getValue func(reader dal.PebbleGetter, canonicalKey []byte) (T, error),
	store dal.PebbleGetter,
	attrCode byte,
	tracker []attributes.U128,
	bloomFilter *bloom.Filter,
	logger logging.Logger,
	typeName string,
) (*resolveResult, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		plans    = make([]*raftcmdpb.AttributeCoverage, 0, len(keys))
	)

	sem := make(chan struct{}, resolveParallelism)

	for id, canonicalKey := range keys {
		// The XXH3-128 id was computed once at Add time and lives in
		// the map key — no rehash. Only the XXH3-64 tag has to be
		// derived here; option E (fused MakeKey) would eliminate this
		// second pass entirely.
		tag := attributes.Tag64(canonicalKey)

		switch attrCache.CheckCache(nextIndex, id) {
		case cache.CacheUnreachable:
			// Admission predicts ≥2 rotations between propose and apply.
			// Any preload computed now would be discarded before the FSM
			// reads it, so reject at admission and let the client retry
			// against a fresher snapshot. Bounded to at most 1 rotation
			// between admission and apply, which the gen0→gen1 read
			// fallback and lazy Del promote handle correctly.
			//
			// Continue processing so wg.Wait() below drains any CacheMiss
			// loader goroutine earlier iterations already launched.
			if logger.Enabled(logging.TraceLevel) {
				logger.WithFields(map[string]any{
					"type":      typeName,
					"key":       hex.EncodeToString(canonicalKey),
					"nextIndex": nextIndex,
					"boundary":  boundary,
				}).Tracef("Cache horizon exceeded: admission rejection")
			}

			mu.Lock()
			if firstErr == nil {
				firstErr = ErrCacheHorizonExceeded
			}
			mu.Unlock()

			continue

		case cache.CacheHit:
			// Cache has the key somewhere (gen0 or gen1). Emit a
			// coverage-only entry — no cache mutation is needed at
			// Preload: Get's gen0→gen1 fallback surfaces the value on
			// read and Del's lazy promote fabricates a gen0 tombstone
			// on delete. No Pebble read required.
			mu.Lock()
			plans = append(plans, coverageEntry(id, attrCode, tag))
			mu.Unlock()

			continue

		case cache.CacheMiss:
			// Bloom filter short-circuit: when the key is definitely not
			// in Pebble, skip the goroutine + Pebble Get and emit Declare
			// (coverage-only, no value to seed).
			if bloomFilter != nil && !bloomFilter.MayContain(id) {
				mu.Lock()
				plans = append(plans, coverageEntry(id, attrCode, tag))
				mu.Unlock()

				continue
			}

			if logger.Enabled(logging.TraceLevel) {
				logger.WithFields(map[string]any{
					"type":      typeName,
					"key":       hex.EncodeToString(canonicalKey),
					"nextIndex": nextIndex,
					"boundary":  boundary,
				}).Tracef("Cache miss: key not guaranteed in cache, loading from store")
			}

			sem <- struct{}{}

			// Loop variables can be captured directly by the goroutine
			// closure since Go 1.22 (per-iteration binding). No need
			// for the id := id shadow trick.

			wg.Go(func() {
				defer func() { <-sem }()

				result, err := loader.LoadOrWait(id, boundary, cacheEpoch, func() (T, error) {
					return getValue(store, canonicalKey)
				})

				mu.Lock()
				defer mu.Unlock()

				if err != nil {
					if firstErr == nil {
						firstErr = err
					}

					return
				}

				if result.FromLoad {
					tracker = append(tracker, id)
				}

				var zero T
				hasValue := any(result.Value) != any(zero)

				// Track bloom false positives: MayContain said "maybe" but Pebble
				// had nothing. Only counts loads we actually performed (FromLoad).
				if result.FromLoad && !hasValue && bloomFilter != nil {
					bloomFilter.RecordFalsePositive()
				}

				if hasValue {
					attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag}

					attrValue, marshalErr := buildPreloadPayload(attrCode, result.Value)
					if marshalErr != nil {
						if firstErr == nil {
							firstErr = marshalErr
						}

						return
					}

					plans = append(plans, seedEntry(attrID, attrCode, attrValue))

					return
				}

				// Pebble had no value either — coverage-only entry. If a
				// concurrent write populated the cache between admission
				// and apply, Get's gen0→gen1 fallback will surface it at
				// apply time (bounded by CacheUnreachable at ≥2 rotations).
				plans = append(plans, coverageEntry(id, attrCode, tag))
			})
		}
	}

	wg.Wait()

	if firstErr != nil {
		// Return the partial tracker alongside the error so the caller's
		// cleanup token still picks up the keys that loaded successfully
		// before another concurrent worker hit firstErr. Without this,
		// transient Pebble or marshal failures leave loader entries
		// pinned for the rest of the process's lifetime — unbounded
		// growth across retries.
		return &resolveResult{tracker: tracker}, firstErr
	}

	return &resolveResult{attributes: plans, tracker: tracker}, nil
}

// buildPreloadPayload marshals value into an AttributeValue envelope.
// The kind (attrCode) lives on the parent AttributeCoverage, not here — the
// FSM dispatches the typed unmarshal via AttributeCoverage.attr_code.
func buildPreloadPayload[V interface {
	MarshalVT() ([]byte, error)
}](attrCode byte, value V) (*raftcmdpb.AttributeValue, error) {
	raw, err := value.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshal preload value (attrCode=0x%x): %w", attrCode, err)
	}

	return &raftcmdpb.AttributeValue{RawValue: raw}, nil
}
