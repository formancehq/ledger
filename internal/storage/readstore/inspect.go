package readstore

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
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
	Ledger      string
	Namespace   string // "a:" or "t:"
	MetadataKey string
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
	prefix := MetadataIndexPrefix(params.KB, params.Ledger, params.Namespace, params.MetadataKey)
	upper := IncrementBytes(prefix)

	lower := prefix
	if len(params.CursorBytes) > 0 {
		seekKey := make([]byte, len(prefix)+len(params.CursorBytes))
		copy(seekKey, prefix)
		copy(seekKey[len(prefix):], params.CursorBytes)
		lower = IncrementBytes(seekKey)
	}

	iter, err := params.Reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating metadata index iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	pageSize := params.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	result := &InspectResult{}
	var prevValueBytes []byte

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= len(prefix) {
			continue
		}

		valueData := key[len(prefix):]

		_, consumed, err := DecodeValue(valueData)
		if err != nil {
			continue
		}

		currentValueBytes := valueData[:consumed]
		if bytes.Equal(currentValueBytes, prevValueBytes) {
			continue
		}

		if uint32(len(result.Values)) >= pageSize {
			result.HasMore = true

			break
		}

		decoded, _, _ := DecodeValue(valueData)
		result.Values = append(result.Values, decoded)
		prevValueBytes = make([]byte, len(currentValueBytes))
		copy(prevValueBytes, currentValueBytes)
		result.NextCursor = prevValueBytes
	}

	return result, nil
}

// inspectFacets scans the metadata index and collects (value, count) pairs with pagination.
func inspectFacets(params InspectParams) (*InspectResult, error) {
	prefix := MetadataIndexPrefix(params.KB, params.Ledger, params.Namespace, params.MetadataKey)
	upper := IncrementBytes(prefix)

	lower := prefix
	if len(params.CursorBytes) > 0 {
		seekKey := make([]byte, len(prefix)+len(params.CursorBytes))
		copy(seekKey, prefix)
		copy(seekKey[len(prefix):], params.CursorBytes)
		lower = IncrementBytes(seekKey)
	}

	iter, err := params.Reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating metadata index iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	pageSize := params.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	result := &InspectResult{}

	var (
		prevValueBytes []byte
		currentValue   *commonpb.MetadataValue
		currentCount   uint64
	)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= len(prefix) {
			continue
		}

		valueData := key[len(prefix):]

		_, consumed, err := DecodeValue(valueData)
		if err != nil {
			continue
		}

		currentValueBytes := valueData[:consumed]

		if bytes.Equal(currentValueBytes, prevValueBytes) {
			currentCount++

			continue
		}

		// Emit previous facet if any.
		if currentValue != nil {
			if uint32(len(result.Facets)) >= pageSize {
				result.HasMore = true

				break
			}

			result.Facets = append(result.Facets, InspectFacetEntry{Value: currentValue, Count: currentCount})
			result.NextCursor = prevValueBytes
		}

		decoded, _, _ := DecodeValue(valueData)
		currentValue = decoded
		currentCount = 1
		prevValueBytes = make([]byte, len(currentValueBytes))
		copy(prevValueBytes, currentValueBytes)
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
	prefix := MetadataIndexPrefix(params.KB, params.Ledger, params.Namespace, params.MetadataKey)
	upper := IncrementBytes(prefix)

	iter, err := params.Reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating metadata index iterator: %w", err)
	}

	var prevValueBytes []byte

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= len(prefix) {
			continue
		}

		valueData := key[len(prefix):]

		_, consumed, decErr := DecodeValue(valueData)
		if decErr != nil {
			continue
		}

		currentValueBytes := valueData[:consumed]

		if bytes.Equal(currentValueBytes, prevValueBytes) {
			continue
		}

		decoded, _, _ := DecodeValue(valueData)

		result.Cardinality++

		if result.Min == nil {
			result.Min = decoded
		}

		result.Max = decoded
		prevValueBytes = make([]byte, len(currentValueBytes))
		copy(prevValueBytes, currentValueBytes)
	}

	_ = iter.Close()

	// Count entities with key (non-null).
	nonNullPrefix := EntityExistsNonNullPrefix(params.KB, params.Ledger, params.Namespace, params.MetadataKey)
	result.EntitiesWithKey, err = countPrefix(params.Reader, nonNullPrefix)

	if err != nil {
		return nil, fmt.Errorf("counting non-null entities: %w", err)
	}

	// Count entities with null value.
	nullPrefix := EntityExistsNullPrefix(params.KB, params.Ledger, params.Namespace, params.MetadataKey)
	result.EntitiesWithNull, err = countPrefix(params.Reader, nullPrefix)

	if err != nil {
		return nil, fmt.Errorf("counting null entities: %w", err)
	}

	return result, nil
}

// countPrefix counts the number of keys with the given prefix.
func countPrefix(reader dal.PebbleReader, prefix []byte) (uint64, error) {
	upper := IncrementBytes(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return 0, err
	}

	defer func() { _ = iter.Close() }()

	var count uint64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count, nil
}
