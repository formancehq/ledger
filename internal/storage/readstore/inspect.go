package readstore

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// InspectMode controls what the scan produces.
type InspectMode int

const (
	InspectDistinctValuesMode InspectMode = iota
	InspectFacetsMode
	InspectSummaryMode
)

const defaultPageSize = 100

// InspectParams holds all parameters for an index inspection scan.
type InspectParams struct {
	Reader      dal.PebbleReader
	KB          *dal.KeyBuilder
	LedgerName  string
	Namespace   string // "a:" or "t:"
	MetadataKey string
	// Version is the per-replica forward-encoding version to scan
	// (IndexVersionState.CurrentVersion). 0 is an invariant — callers
	// must resolve a non-zero current_version before calling Inspect
	// (or short-circuit and return "index not built locally").
	Version     uint32
	Mode        InspectMode
	PageSize    uint32
	CursorBytes []byte // decoded opaque cursor (nil = start)
}

// InspectResult holds the scan results.
type InspectResult struct {
	Values           []*commonpb.MetadataValue
	Facets           []InspectFacetEntry
	Cardinality      uint64
	Min              *commonpb.MetadataValue
	Max              *commonpb.MetadataValue
	EntitiesWithKey  uint64
	EntitiesWithNull uint64
	HasMore          bool
	NextCursor       []byte
}

// InspectFacetEntry is a (value, count) pair.
type InspectFacetEntry struct {
	Value *commonpb.MetadataValue
	Count uint64
}

// forEachLiveGroup walks event keys in [lower, upper) and invokes fn for
// every group (the bytes between prefixLen and the entity terminator) whose
// latest event is an ADD — i.e. current membership. fn returns false to stop
// early; its argument is only valid for the duration of the call.
//
// A key it cannot read is an error, not a skipped row: statistics derived from
// the events around it would be plausible and wrong, hiding the corruption
// they were computed over.
func forEachLiveGroup(reader dal.PebbleReader, lower, upper []byte, prefixLen int, fn func(group []byte) bool) error {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}

	defer func() { _ = iter.Close() }()

	suffix := metadataEventSuffixLen + 1

	var (
		group   []byte
		started bool
		live    bool
	)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		tpos := len(key) - suffix
		if tpos < prefixLen || key[tpos] != metadataEventTerminator {
			return fmt.Errorf("malformed metadata event key %x", key)
		}

		g := key[prefixLen:tpos]
		if !started || !bytes.Equal(g, group) {
			if started && live && !fn(group) {
				return iter.Error()
			}

			group = append(group[:0], g...)
			started = true
		}

		if !validEventOp(key[tpos+9]) {
			return fmt.Errorf("malformed metadata event key %x", key)
		}

		// Events are seq-ascending within a group: the last op wins.
		live = key[tpos+9] == MetadataEventAdd
	}

	if started && live {
		_ = fn(group)
	}

	return iter.Error()
}

// countLiveGroups counts current members under an event prefix.
func countLiveGroups(reader dal.PebbleReader, prefix []byte) (uint64, error) {
	var n uint64

	err := forEachLiveGroup(reader, prefix, IncrementBytes(prefix), len(prefix), func([]byte) bool {
		n++

		return true
	})

	return n, err
}

// InspectIndex scans a metadata index and returns statistics or values.
func InspectIndex(params InspectParams) (*InspectResult, error) {
	switch params.Mode {
	case InspectDistinctValuesMode:
		return inspectDistinctValues(params)
	case InspectFacetsMode:
		return inspectFacets(params)
	case InspectSummaryMode:
		return inspectSummary(params)
	default:
		return nil, fmt.Errorf("unknown inspect mode: %d", params.Mode)
	}
}

// inspectDistinctValues scans the metadata index and collects unique values with pagination.
func inspectDistinctValues(params InspectParams) (*InspectResult, error) {
	prefix := MetadataIndexPrefixV(params.KB, params.LedgerName, params.Namespace, params.MetadataKey, params.Version)
	upper := IncrementBytes(prefix)

	lower := prefix
	if len(params.CursorBytes) > 0 {
		seekKey := make([]byte, len(prefix)+len(params.CursorBytes))
		copy(seekKey, prefix)
		copy(seekKey[len(prefix):], params.CursorBytes)
		lower = IncrementBytes(seekKey)
	}

	pageSize := params.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	result := &InspectResult{}

	var (
		prevValueBytes []byte
		decodeErr      error
	)

	err := forEachLiveGroup(params.Reader, lower, upper, len(prefix), func(group []byte) bool {
		_, consumed, decErr := DecodeValue(group)
		if decErr != nil {
			decodeErr = fmt.Errorf("malformed metadata event group %x: %w", group, decErr)

			return false
		}

		currentValueBytes := group[:consumed]
		if bytes.Equal(currentValueBytes, prevValueBytes) {
			return true
		}

		if uint32(len(result.Values)) >= pageSize {
			result.HasMore = true

			return false
		}

		decoded, _, _ := DecodeValue(group)
		result.Values = append(result.Values, decoded)
		prevValueBytes = make([]byte, len(currentValueBytes))
		copy(prevValueBytes, currentValueBytes)
		result.NextCursor = prevValueBytes

		return true
	})
	if err != nil {
		return nil, fmt.Errorf("scanning metadata index events: %w", err)
	}

	if decodeErr != nil {
		return nil, decodeErr
	}

	return result, nil
}

// inspectFacets scans the metadata index and collects (value, count) pairs with pagination.
func inspectFacets(params InspectParams) (*InspectResult, error) {
	prefix := MetadataIndexPrefixV(params.KB, params.LedgerName, params.Namespace, params.MetadataKey, params.Version)
	upper := IncrementBytes(prefix)

	lower := prefix
	if len(params.CursorBytes) > 0 {
		seekKey := make([]byte, len(prefix)+len(params.CursorBytes))
		copy(seekKey, prefix)
		copy(seekKey[len(prefix):], params.CursorBytes)
		lower = IncrementBytes(seekKey)
	}

	pageSize := params.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	result := &InspectResult{}

	var (
		prevValueBytes []byte
		currentValue   *commonpb.MetadataValue
		currentCount   uint64
		decodeErr      error
	)

	err := forEachLiveGroup(params.Reader, lower, upper, len(prefix), func(group []byte) bool {
		_, consumed, decErr := DecodeValue(group)
		if decErr != nil {
			decodeErr = fmt.Errorf("malformed metadata event group %x: %w", group, decErr)

			return false
		}

		currentValueBytes := group[:consumed]

		if bytes.Equal(currentValueBytes, prevValueBytes) {
			currentCount++

			return true
		}

		// Emit previous facet if any.
		if currentValue != nil {
			if uint32(len(result.Facets)) >= pageSize {
				result.HasMore = true

				return false
			}

			result.Facets = append(result.Facets, InspectFacetEntry{Value: currentValue, Count: currentCount})
			result.NextCursor = prevValueBytes
		}

		decoded, _, _ := DecodeValue(group)
		currentValue = decoded
		currentCount = 1
		prevValueBytes = make([]byte, len(currentValueBytes))
		copy(prevValueBytes, currentValueBytes)

		return true
	})
	if err != nil {
		return nil, fmt.Errorf("scanning metadata index events: %w", err)
	}

	if decodeErr != nil {
		return nil, decodeErr
	}

	// Emit last facet.
	if currentValue != nil && !result.HasMore {
		if uint32(len(result.Facets)) < pageSize {
			result.Facets = append(result.Facets, InspectFacetEntry{Value: currentValue, Count: currentCount})
			result.NextCursor = prevValueBytes
		} else {
			result.HasMore = true
		}
	}

	return result, nil
}

// inspectSummary performs a full scan to compute cardinality, min, max, and existence counts.
func inspectSummary(params InspectParams) (*InspectResult, error) {
	result := &InspectResult{}

	// Scan metadata index for cardinality, min, max.
	prefix := MetadataIndexPrefixV(params.KB, params.LedgerName, params.Namespace, params.MetadataKey, params.Version)
	upper := IncrementBytes(prefix)

	var (
		prevValueBytes []byte
		decodeErr      error
	)

	err := forEachLiveGroup(params.Reader, prefix, upper, len(prefix), func(group []byte) bool {
		_, consumed, decErr := DecodeValue(group)
		if decErr != nil {
			decodeErr = fmt.Errorf("malformed metadata event group %x: %w", group, decErr)

			return false
		}

		currentValueBytes := group[:consumed]

		if bytes.Equal(currentValueBytes, prevValueBytes) {
			return true
		}

		decoded, _, _ := DecodeValue(group)

		result.Cardinality++

		if result.Min == nil {
			result.Min = decoded
		}

		result.Max = decoded
		prevValueBytes = make([]byte, len(currentValueBytes))
		copy(prevValueBytes, currentValueBytes)

		return true
	})
	if err != nil {
		return nil, fmt.Errorf("scanning metadata index events: %w", err)
	}

	if decodeErr != nil {
		return nil, decodeErr
	}

	// Count entities with key (non-null).
	nonNullPrefix := EntityExistsNonNullPrefixV(params.KB, params.LedgerName, params.Namespace, params.MetadataKey, params.Version)
	result.EntitiesWithKey, err = countLiveGroups(params.Reader, nonNullPrefix)

	if err != nil {
		return nil, fmt.Errorf("counting non-null entities: %w", err)
	}

	// Count entities with null value.
	nullPrefix := EntityExistsNullPrefixV(params.KB, params.LedgerName, params.Namespace, params.MetadataKey, params.Version)
	result.EntitiesWithNull, err = countLiveGroups(params.Reader, nullPrefix)

	if err != nil {
		return nil, fmt.Errorf("counting null entities: %w", err)
	}

	return result, nil
}
