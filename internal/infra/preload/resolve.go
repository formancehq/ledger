package preload

import (
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// resolveStandard resolves a standard attribute type (loaded via attrs.*.ComputeValue).
// For each key not guaranteed in cache, it loads (or waits for) the value and
// appends a preload entry. Returns updated tracker keys.
func resolveStandard[K interface {
	comparable
	Bytes() []byte
}, T any](
	keys map[K]struct{},
	nextIndex, boundary uint64,
	attrCache *cache.AttributeCache[T],
	loader *AttributeLoader[T],
	computeValue func(reader dal.PebbleReader, index uint64, canonicalKey []byte) (T, uint64, error),
	store dal.PebbleReader,
	buildPreload func(id *raftcmdpb.AttributeID, value T) *raftcmdpb.Preload,
	alwaysSend bool,
	tracker []attributes.U128,
) ([]*raftcmdpb.Preload, []attributes.U128, error) {
	var preloads []*raftcmdpb.Preload

	for key := range keys {
		canonicalKey := key.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if attrCache.IsGuaranteedInCache(nextIndex, id) {
			continue
		}

		result, err := loader.LoadOrWait(id, boundary, func() (T, uint64, error) {
			return computeValue(store, boundary, canonicalKey)
		})
		if err != nil {
			return nil, nil, err
		}

		if result.FromLoad {
			tracker = append(tracker, id)
		}

		var zero T
		hasValue := any(result.Value) != any(zero)

		if alwaysSend || hasValue {
			attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag, BaseIndex: result.BaseIndex}
			preloads = append(preloads, buildPreload(attrID, result.Value))
		}
	}

	return preloads, tracker, nil
}

// resolveCustom resolves a custom attribute type where callers provide load functions.
// Custom types (sink configs, numscript) don't come from Pebble attributes, so
// baseIndex is always 0 — acceptable since they are rarely overwritten.
func resolveCustom[K interface {
	comparable
	Bytes() []byte
}, T any](
	keys map[K]func() (T, error),
	nextIndex, boundary uint64,
	attrCache *cache.AttributeCache[T],
	loader *AttributeLoader[T],
	buildPreload func(id *raftcmdpb.AttributeID, value T) *raftcmdpb.Preload,
	alwaysSend bool,
	tracker []attributes.U128,
) ([]*raftcmdpb.Preload, []attributes.U128, error) {
	var preloads []*raftcmdpb.Preload

	for key, loadFn := range keys {
		canonicalKey := key.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if attrCache.IsGuaranteedInCache(nextIndex, id) {
			continue
		}

		// Wrap loadFn to match the (T, uint64, error) signature — baseIndex=0 for custom types.
		wrappedFn := func() (T, uint64, error) {
			v, err := loadFn()
			return v, 0, err
		}

		result, err := loader.LoadOrWait(id, boundary, wrappedFn)
		if err != nil {
			return nil, nil, err
		}

		if result.FromLoad {
			tracker = append(tracker, id)
		}

		var zero T
		hasValue := any(result.Value) != any(zero)

		if alwaysSend || hasValue {
			attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag}
			preloads = append(preloads, buildPreload(attrID, result.Value))
		}
	}

	return preloads, tracker, nil
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
