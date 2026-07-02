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
// per-attribute plans plus the tracker keys. Each AttributePlan carries
// exactly one intent — Value (loaded from Pebble), Touch (Gen1→Gen0
// promotion at apply), or Declare (already in Gen0 or verified absent —
// coverage only). The FSM-side preload.View consumes the whole list as
// its coverage set so reads on declared keys never trip a false-positive
// "not preloaded" miss.
//
// Idempotency keys live on the parallel idempotencyKeys slice — they are
// NOT cache attributes (the FSM applies them to a dedicated store) so
// they bypass the AttributePlan list and ship on ExecutionPlan.idempotency_keys.
type resolveResult struct {
	attributes      []*raftcmdpb.AttributePlan
	idempotencyKeys []*raftcmdpb.ReloadIdempotencyKey
	tracker         []attributes.U128
}

// declarePlan returns an AttributePlan whose intent is Declare: the key
// is either already in Gen0 or verified absent, so no FSM-side cache
// mutation is required.
func declarePlan(id attributes.U128, attrCode byte, tag uint64) *raftcmdpb.AttributePlan {
	return &raftcmdpb.AttributePlan{
		Id:       &raftcmdpb.AttributeID{Id: id[:], Tag: tag},
		AttrCode: uint32(attrCode),
		Intent:   &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
	}
}

// touchPlan returns an AttributePlan whose intent is Touch: the FSM must
// promote this key from Gen1 to Gen0 before order handlers run.
func touchPlan(id attributes.U128, attrCode byte, tag uint64) *raftcmdpb.AttributePlan {
	return &raftcmdpb.AttributePlan{
		Id:       &raftcmdpb.AttributeID{Id: id[:], Tag: tag},
		AttrCode: uint32(attrCode),
		Intent:   &raftcmdpb.AttributePlan_Touch{Touch: &raftcmdpb.Touch{}},
	}
}

// preloadPlan wraps a typed AttributeValue into an AttributePlan. attr_code
// lives on the plan envelope so the FSM apply path routes the dispatch
// without unwrapping the oneof.
func preloadPlan(attrID *raftcmdpb.AttributeID, attrCode byte, value *raftcmdpb.AttributeValue) *raftcmdpb.AttributePlan {
	return &raftcmdpb.AttributePlan{
		Id:       attrID,
		AttrCode: uint32(attrCode),
		Intent:   &raftcmdpb.AttributePlan_Value{Value: value},
	}
}

// resolveAttributePreload resolves one attribute cache for the plan
// pipeline. Keys are passed as canonical byte strings (see AttributeSet
// on Needs) — no K generic; the (attrCode, canonical) pair is the whole
// identity.
//
// For each key, resolveAttributePreload emits ONE AttributePlan based on
// admission's CheckCache verdict:
//
//   - CacheUnreachable → ErrCacheHorizonExceeded (admission rejection).
//   - CacheGuaranteed → Declare (cache already has the key in Gen0 at
//     apply time — no cache mutation needed).
//   - CacheNeedsTouch → Touch (key sits in Gen1; the FSM promotes it back
//     to Gen0 before order handlers run).
//   - CacheMiss + bloom/Pebble-absent → Declare (coverage-only).
//   - CacheMiss + Pebble-load-hit → Value(v) (MirrorPreload seeds gen0+gen1).
//
// Keys are resolved with bounded parallelism to amortize I/O latency.
func resolveAttributePreload[T interface {
	MarshalVT() ([]byte, error)
}](
	keys map[string]struct{},
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
		plans    []*raftcmdpb.AttributePlan
	)

	sem := make(chan struct{}, resolveParallelism)

	for key := range keys {
		canonicalKey := []byte(key)
		id, tag := attributes.MakeKey(canonicalKey)

		switch attrCache.CheckCache(nextIndex, id) {
		case cache.CacheUnreachable:
			// Admission predicts ≥2 rotations between propose and apply: a
			// preload computed now would be rotated out before the FSM reads
			// it. Record the rejection but continue processing so wg.Wait()
			// below drains any CacheMiss loader goroutine earlier iterations
			// already launched.
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

		case cache.CacheGuaranteed:
			// Key is already in Gen0 and will still be there at apply.
			// Emit Declare — the FSM's coverage view admits the read; no
			// mutation required.
			mu.Lock()
			plans = append(plans, declarePlan(id, attrCode, tag))
			mu.Unlock()

			continue

		case cache.CacheNeedsTouch:
			// Key sits in Gen1 (predicted-apply lands in the next
			// generation). Emit Touch so the FSM promotes Gen1 → Gen0
			// before the order runs — otherwise the next rotation would
			// drop the entry.
			if logger.Enabled(logging.TraceLevel) {
				logger.WithFields(map[string]any{
					"type":      typeName,
					"key":       hex.EncodeToString(canonicalKey),
					"nextIndex": nextIndex,
					"boundary":  boundary,
				}).Tracef("Cache touch: promoting key from gen1 to gen0")
			}

			mu.Lock()
			plans = append(plans, touchPlan(id, attrCode, tag))
			mu.Unlock()

			continue

		case cache.CacheMiss:
			// Bloom filter short-circuit: when the key is definitely not
			// in Pebble, skip the goroutine + Pebble Get and emit Declare
			// (coverage-only, no value to seed).
			if bloomFilter != nil && !bloomFilter.MayContain(id) {
				mu.Lock()
				plans = append(plans, declarePlan(id, attrCode, tag))
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

			canonicalKey := canonicalKey
			id := id
			tag := tag

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

					plans = append(plans, preloadPlan(attrID, attrCode, attrValue))

					return
				}

				// Pebble had no value either — coverage-only Declare.
				plans = append(plans, declarePlan(id, attrCode, tag))
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
// The kind (attrCode) lives on the parent AttributePlan, not here — the
// FSM dispatches the typed unmarshal via AttributePlan.attr_code.
func buildPreloadPayload[V interface {
	MarshalVT() ([]byte, error)
}](attrCode byte, value V) (*raftcmdpb.AttributeValue, error) {
	raw, err := value.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshal preload value (attrCode=0x%x): %w", attrCode, err)
	}

	return &raftcmdpb.AttributeValue{RawValue: raw}, nil
}
