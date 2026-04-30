package preload

import (
	"encoding/hex"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/bloom"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// resolveParallelism is the maximum number of concurrent Pebble Gets within
// a single attribute type resolution. The workload is I/O bound (Ceph RBD
// reads at ~1ms per miss), so moderate parallelism saturates the storage
// without overwhelming Pebble's iterator pool.
const resolveParallelism = 16

// resolveResult holds the output of a resolve call: preloads, touches, and tracker keys.
type resolveResult struct {
	preloads []*raftcmdpb.Preload
	touches  []*raftcmdpb.CacheTouch
	tracker  []attributes.U128
}

// resolveStandard resolves a standard attribute type (loaded via attrs.*.Get).
// For each key not guaranteed in cache, it either emits a touch (if the key is
// in Gen1 but not Gen0) or loads the value from store and appends a preload entry.
// Keys are resolved with bounded parallelism to amortize I/O latency.
func resolveStandard[K interface {
	comparable
	Bytes() []byte
}, T any](
	keys map[K]struct{},
	nextIndex, boundary uint64,
	attrCache *cache.AttributeCache[T],
	loader *AttributeLoader[T],
	getValue func(reader dal.PebbleReader, canonicalKey []byte) (T, error),
	store dal.PebbleReader,
	buildPreload func(id *raftcmdpb.AttributeID, value T) *raftcmdpb.Preload,
	includeZeroValue bool,
	touchType raftcmdpb.CacheTouchType,
	tracker []attributes.U128,
	bloomFilter *bloom.Filter,
	logger logging.Logger,
	typeName string,
) (*resolveResult, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		preloads []*raftcmdpb.Preload
		touches  []*raftcmdpb.CacheTouch
	)

	sem := make(chan struct{}, resolveParallelism)

	for key := range keys {
		canonicalKey := key.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		switch attrCache.CheckCache(nextIndex, id) {
		case cache.CacheGuaranteed:
			continue

		case cache.CacheNeedsTouch:
			if logger.Enabled(logging.DebugLevel) {
				logger.WithFields(map[string]any{
					"type":      typeName,
					"key":       hex.EncodeToString(canonicalKey),
					"nextIndex": nextIndex,
					"boundary":  boundary,
				}).Debugf("Cache touch: promoting key from gen1 to gen0")
			}

			mu.Lock()
			touches = append(touches, &raftcmdpb.CacheTouch{
				Id:   id[:],
				Type: touchType,
			})
			mu.Unlock()

			continue

		case cache.CacheMiss:
			// Bloom filter short-circuit: if the key is definitely not in Pebble,
			// skip the goroutine + Pebble Get and return a zero value directly.
			if bloomFilter != nil && !bloomFilter.MayContain(id) {
				if includeZeroValue {
					var zero T
					attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag}

					mu.Lock()
					preloads = append(preloads, buildPreload(attrID, zero))
					mu.Unlock()
				}

				continue
			}

			if logger.Enabled(logging.DebugLevel) {
				logger.WithFields(map[string]any{
					"type":      typeName,
					"key":       hex.EncodeToString(canonicalKey),
					"nextIndex": nextIndex,
					"boundary":  boundary,
				}).Debugf("Cache miss: key not guaranteed in cache, loading from store")
			}

			sem <- struct{}{}

			canonicalKey := canonicalKey
			id := id
			tag := tag

			wg.Go(func() {
				defer func() { <-sem }()

				result, err := loader.LoadOrWait(id, boundary, func() (T, error) {
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

				if includeZeroValue || hasValue {
					attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag}
					preloads = append(preloads, buildPreload(attrID, result.Value))
				}
			})
		}
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return &resolveResult{preloads: preloads, touches: touches, tracker: tracker}, nil
}

// resolveCustom resolves a custom attribute type where callers provide load functions.
// Custom types (sink configs, numscript) don't come from Pebble attributes, so
// baseIndex is always 0 — acceptable since they are rarely overwritten.
// Keys are resolved with bounded parallelism to amortize I/O latency.
func resolveCustom[K interface {
	comparable
	Bytes() []byte
}, T any](
	keys map[K]func() (T, error),
	nextIndex, boundary uint64,
	attrCache *cache.AttributeCache[T],
	loader *AttributeLoader[T],
	buildPreload func(id *raftcmdpb.AttributeID, value T) *raftcmdpb.Preload,
	includeZeroValue bool,
	touchType raftcmdpb.CacheTouchType,
	tracker []attributes.U128,
	logger logging.Logger,
	typeName string,
) (*resolveResult, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		preloads []*raftcmdpb.Preload
		touches  []*raftcmdpb.CacheTouch
	)

	sem := make(chan struct{}, resolveParallelism)

	for key, loadFn := range keys {
		canonicalKey := key.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		switch attrCache.CheckCache(nextIndex, id) {
		case cache.CacheGuaranteed:
			continue

		case cache.CacheNeedsTouch:
			if logger.Enabled(logging.DebugLevel) {
				logger.WithFields(map[string]any{
					"type":      typeName,
					"key":       hex.EncodeToString(canonicalKey),
					"nextIndex": nextIndex,
					"boundary":  boundary,
				}).Debugf("Cache touch: promoting key from gen1 to gen0")
			}

			mu.Lock()
			touches = append(touches, &raftcmdpb.CacheTouch{
				Id:   id[:],
				Type: touchType,
			})
			mu.Unlock()

			continue

		case cache.CacheMiss:
			if logger.Enabled(logging.DebugLevel) {
				logger.WithFields(map[string]any{
					"type":      typeName,
					"key":       hex.EncodeToString(canonicalKey),
					"nextIndex": nextIndex,
					"boundary":  boundary,
				}).Debugf("Cache miss: key not guaranteed in cache, loading from store")
			}

			sem <- struct{}{}

			loadFn := loadFn
			id := id
			tag := tag

			wg.Go(func() {
				defer func() { <-sem }()

				result, err := loader.LoadOrWait(id, boundary, loadFn)

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

				if includeZeroValue || hasValue {
					attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag}
					preloads = append(preloads, buildPreload(attrID, result.Value))
				}
			})
		}
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return &resolveResult{preloads: preloads, touches: touches, tracker: tracker}, nil
}

// Preload builders — one per attribute type.

func buildLedgerPreload(id *raftcmdpb.AttributeID, info *commonpb.LedgerInfo) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_Ledger{
			Ledger: &raftcmdpb.PreloadLedger{
				Id:   id,
				Info: info,
			},
		},
	}
}

func buildBoundaryPreload(id *raftcmdpb.AttributeID, boundaries *raftcmdpb.LedgerBoundaries) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_Boundary{
			Boundary: &raftcmdpb.PreloadBoundary{
				Id:         id,
				Boundaries: boundaries,
			},
		},
	}
}

func buildVolumePreload(id *raftcmdpb.AttributeID, vol *raftcmdpb.VolumePair) *raftcmdpb.Preload {
	var preloadInput, preloadOutput *commonpb.Uint256
	if vol != nil {
		preloadInput = vol.GetInput()
		preloadOutput = vol.GetOutput()
	}

	if preloadInput == nil {
		preloadInput = commonpb.NewUint256FromUint64(0)
	}

	if preloadOutput == nil {
		preloadOutput = commonpb.NewUint256FromUint64(0)
	}

	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_Volume{
			Volume: &raftcmdpb.PreloadVolume{
				Id:     id,
				Input:  preloadInput,
				Output: preloadOutput,
			},
		},
	}
}

func buildIdempotencyKeyPreload(id *raftcmdpb.AttributeID, value *commonpb.IdempotencyKeyValue) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_IdempotencyKey{
			IdempotencyKey: &raftcmdpb.PreloadIdempotencyKey{
				Id:          id,
				LogSequence: value.GetLogSequence(),
				Hash:        value.GetHash(),
			},
		},
	}
}

func buildReferencePreload(id *raftcmdpb.AttributeID, value *commonpb.TransactionReferenceValue) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_TransactionReference{
			TransactionReference: &raftcmdpb.PreloadTransactionReference{
				Id:            id,
				TransactionId: value.GetTransactionId(),
			},
		},
	}
}

func buildSinkConfigPreload(id *raftcmdpb.AttributeID, config *commonpb.SinkConfig) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_SinkConfig{
			SinkConfig: &raftcmdpb.PreloadSinkConfig{
				Id:     id,
				Config: config,
			},
		},
	}
}

func buildNumscriptVersionPreload(id *raftcmdpb.AttributeID, version string) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_NumscriptVersion{
			NumscriptVersion: &raftcmdpb.PreloadNumscriptVersion{
				Id:      id,
				Version: version,
			},
		},
	}
}

func buildNumscriptEntryPreload(id *raftcmdpb.AttributeID, exists bool) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_NumscriptEntry{
			NumscriptEntry: &raftcmdpb.PreloadNumscriptEntry{
				Id:     id,
				Exists: exists,
			},
		},
	}
}

func buildMetadataPreload(id *raftcmdpb.AttributeID, value *commonpb.MetadataValue) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_AccountMetadata{
			AccountMetadata: &raftcmdpb.PreloadAccountMetadata{
				Id:    id,
				Value: value,
			},
		},
	}
}

func buildNumscriptParsedPreload(id *raftcmdpb.AttributeID, plain string) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_NumscriptParsed{
			NumscriptParsed: &raftcmdpb.PreloadNumscriptParsed{
				Id:    id,
				Plain: plain,
			},
		},
	}
}

func buildTransactionStatePreload(id *raftcmdpb.AttributeID, value *commonpb.TransactionState) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_TransactionState{
			TransactionState: &raftcmdpb.PreloadTransactionState{
				Id:    id,
				State: value,
			},
		},
	}
}
