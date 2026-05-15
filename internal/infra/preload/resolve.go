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

// resolveAttributePreload resolves a standard attribute type (loaded via attrs.*.Get).
// For each key not guaranteed in cache, it either emits a touch (if the key is
// in Gen1 but not Gen0) or loads the value from store and appends a preload entry.
// Keys are resolved with bounded parallelism to amortize I/O latency.
func resolveAttributePreload[K interface {
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
				Id:       id[:],
				AttrType: uint32(attrCode),
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

// Preload builders — one per attribute type.

func buildLedgerPreload(id *raftcmdpb.AttributeID, value *commonpb.LedgerInfo) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_Ledger{Ledger: &raftcmdpb.PreloadLedger{Id: id, Value: value}}}
}

func buildBoundaryPreload(id *raftcmdpb.AttributeID, value *raftcmdpb.LedgerBoundaries) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_Boundary{Boundary: &raftcmdpb.PreloadBoundary{Id: id, Value: value}}}
}

func buildVolumePreload(id *raftcmdpb.AttributeID, vol *raftcmdpb.VolumePair) *raftcmdpb.Preload {
	if vol == nil {
		vol = &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(0),
			Output: commonpb.NewUint256FromUint64(0),
		}
	}

	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_Volume{Volume: &raftcmdpb.PreloadVolume{Id: id, Value: vol}}}
}

func buildReferencePreload(id *raftcmdpb.AttributeID, value *commonpb.TransactionReferenceValue) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_TransactionReference{TransactionReference: &raftcmdpb.PreloadTransactionReference{Id: id, Value: value}}}
}

func buildSinkConfigPreload(id *raftcmdpb.AttributeID, value *commonpb.SinkConfig) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_SinkConfig{SinkConfig: &raftcmdpb.PreloadSinkConfig{Id: id, Value: value}}}
}

func buildNumscriptVersionPreload(id *raftcmdpb.AttributeID, value *commonpb.NumscriptVersionValue) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_NumscriptVersion{NumscriptVersion: &raftcmdpb.PreloadNumscriptVersion{Id: id, Value: value}}}
}

func buildMetadataPreload(id *raftcmdpb.AttributeID, value *commonpb.MetadataValue) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_AccountMetadata{AccountMetadata: &raftcmdpb.PreloadAccountMetadata{Id: id, Value: value}}}
}

func buildTransactionStatePreload(id *raftcmdpb.AttributeID, value *commonpb.TransactionState) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_TransactionState{TransactionState: &raftcmdpb.PreloadTransactionState{Id: id, Value: value}}}
}

func buildNumscriptContentPreload(id *raftcmdpb.AttributeID, value *commonpb.NumscriptInfo) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_NumscriptContent{NumscriptContent: &raftcmdpb.PreloadNumscriptContent{Id: id, Value: value}}}
}

func buildPreparedQueryPreload(id *raftcmdpb.AttributeID, value *commonpb.PreparedQuery) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_PreparedQuery{PreparedQuery: &raftcmdpb.PreloadPreparedQuery{Id: id, Value: value}}}
}

func buildLedgerMetadataPreload(id *raftcmdpb.AttributeID, value *commonpb.MetadataValue) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{Type: &raftcmdpb.Preload_LedgerMetadata{LedgerMetadata: &raftcmdpb.PreloadLedgerMetadata{Id: id, Value: value}}}
}
