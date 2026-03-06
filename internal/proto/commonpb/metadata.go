package commonpb

import (
	"fmt"
	"math"
	"sort"

	"github.com/formancehq/go-libs/v3/metadata"

	jsonPkg "github.com/formancehq/ledger-v3-poc/internal/adapter/json"
)

// Target type constants.
const (
	MetaTargetTypeAccount     = "ACCOUNT"
	MetaTargetTypeTransaction = "TRANSACTION"
)

// MetadataFromMap converts a metadata.Metadata (map[string]string) to a []*Metadata slice.
func MetadataFromMap(m metadata.Metadata) []*Metadata {
	if m == nil {
		return nil
	}

	result := make([]*Metadata, 0, len(m))
	for k, v := range m {
		result = append(result, &Metadata{Key: k, Value: NewStringValue(v)})
	}
	// Sort by key for deterministic output
	sort.Slice(result, func(i, j int) bool {
		return result[i].GetKey() < result[j].GetKey()
	})

	return result
}

// MetadataSetFromMap converts a metadata.Metadata (map[string]string) to a *MetadataSet.
func MetadataSetFromMap(m metadata.Metadata) *MetadataSet {
	if m == nil {
		return nil
	}

	return &MetadataSet{
		Metadata: MetadataFromMap(m),
	}
}

// MetadataToMap converts a []*Metadata slice to metadata.Metadata (map[string]string).
func MetadataToMap(m []*Metadata) metadata.Metadata {
	if m == nil {
		return nil
	}

	result := make(metadata.Metadata, len(m))
	for _, md := range m {
		if md != nil && md.GetValue() != nil {
			result[md.GetKey()] = MetadataValueToString(md.GetValue())
		}
	}

	return result
}

// MetadataSetToMap converts a *MetadataSet to metadata.Metadata (map[string]string).
func MetadataSetToMap(ms *MetadataSet) metadata.Metadata {
	if ms == nil {
		return nil
	}

	return MetadataToMap(ms.GetMetadata())
}

// NewMetadataSet creates a new MetadataSet from a metadata.Metadata map.
func NewMetadataSet(m metadata.Metadata) *MetadataSet {
	return MetadataSetFromMap(m)
}

// ToMap converts a MetadataSet to metadata.Metadata (map[string]string).
func (ms *MetadataSet) ToMap() metadata.Metadata {
	return MetadataSetToMap(ms)
}

// AccountMetadataFromMap converts a map[string]metadata.Metadata to map[string]*MetadataSet.
func AccountMetadataFromMap(m map[string]metadata.Metadata) map[string]*MetadataSet {
	if m == nil {
		return nil
	}

	result := make(map[string]*MetadataSet, len(m))
	for k, v := range m {
		result[k] = MetadataSetFromMap(v)
	}

	return result
}

// AccountMetadataToMap converts a map[string]*MetadataSet to map[string]metadata.Metadata.
func AccountMetadataToMap(m map[string]*MetadataSet) map[string]metadata.Metadata {
	if m == nil {
		return nil
	}

	result := make(map[string]metadata.Metadata, len(m))
	for k, v := range m {
		result[k] = MetadataSetToMap(v)
	}

	return result
}

// MetadataValueToAny converts a MetadataValue to a JSON-compatible any value.
// string_value → string, int_value → int64, uint_value → uint64,
// bool_value → bool, null_value → nil.
func MetadataValueToAny(v *MetadataValue) any {
	if v == nil {
		return nil
	}

	switch t := v.GetType().(type) {
	case *MetadataValue_StringValue:
		return t.StringValue
	case *MetadataValue_IntValue:
		return t.IntValue
	case *MetadataValue_UintValue:
		return t.UintValue
	case *MetadataValue_BoolValue:
		return t.BoolValue
	case *MetadataValue_NullValue:
		return nil
	default:
		return nil
	}
}

// MetadataSetToAnyMap converts a *MetadataSet to map[string]any with typed values.
func MetadataSetToAnyMap(ms *MetadataSet) map[string]any {
	if ms == nil {
		return nil
	}

	result := make(map[string]any, len(ms.GetMetadata()))
	for _, md := range ms.GetMetadata() {
		if md != nil && md.GetValue() != nil {
			result[md.GetKey()] = MetadataValueToAny(md.GetValue())
		}
	}

	return result
}

// MetadataValueFromAny infers a MetadataValue from a JSON-decoded any value.
// Per RFC section 5.2:
//   - bool → bool_value
//   - positive integer (fits uint64) → uint_value
//   - negative integer (fits int64) → int_value
//   - string → string_value
//   - float with decimal → error (floats not supported)
//   - nil → signals deletion (returned as nil)
//   - object/array → error
func MetadataValueFromAny(v any) (*MetadataValue, error) {
	switch val := v.(type) {
	case nil:
		return nil, nil // nil signals key deletion
	case bool:
		return NewBoolValue(val), nil
	case string:
		return NewStringValue(val), nil
	case float64:
		// JSON numbers are decoded as float64 by default
		if val != math.Trunc(val) {
			return nil, fmt.Errorf("float values are not supported for metadata, got %v", val)
		}

		if val < 0 {
			if val < math.MinInt64 {
				return nil, fmt.Errorf("integer value %v overflows int64", val)
			}

			return NewIntValue(int64(val)), nil
		}

		if val > math.MaxUint64 {
			return nil, fmt.Errorf("integer value %v overflows uint64", val)
		}

		return NewUintValue(uint64(val)), nil
	case int64:
		if val < 0 {
			return NewIntValue(val), nil
		}

		return NewUintValue(uint64(val)), nil
	case uint64:
		return NewUintValue(val), nil
	case int:
		if val < 0 {
			return NewIntValue(int64(val)), nil
		}

		return NewUintValue(uint64(val)), nil
	default:
		return nil, fmt.Errorf("unsupported metadata value type %T (objects and arrays are not supported)", v)
	}
}

// MetadataFromAnyMap converts a map[string]any to []*Metadata with JSON type inference.
// Keys with nil values are skipped (nil signals deletion at the HTTP layer).
func MetadataFromAnyMap(m map[string]any) ([]*Metadata, error) {
	if m == nil {
		return nil, nil
	}

	result := make([]*Metadata, 0, len(m))
	for k, v := range m {
		mv, err := MetadataValueFromAny(v)
		if err != nil {
			return nil, fmt.Errorf("metadata key %q: %w", k, err)
		}

		if mv == nil {
			continue // nil means delete this key
		}

		result = append(result, &Metadata{Key: k, Value: mv})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].GetKey() < result[j].GetKey()
	})

	return result, nil
}

// MetadataSetFromAnyMap converts a map[string]any to *MetadataSet with JSON type inference.
func MetadataSetFromAnyMap(m map[string]any) (*MetadataSet, error) {
	if m == nil {
		return nil, nil
	}

	md, err := MetadataFromAnyMap(m)
	if err != nil {
		return nil, err
	}

	return &MetadataSet{Metadata: md}, nil
}

// MarshalJSON implements json.Marshaler for MetadataSet.
// Outputs a flat JSON object with typed values: {"key": "str", "count": 42, "active": true}.
func (ms *MetadataSet) MarshalJSON() ([]byte, error) {
	m := MetadataSetToAnyMap(ms)
	if m == nil {
		m = make(map[string]any)
	}

	return jsonPkg.Marshal(m)
}

// UnmarshalJSON implements json.Unmarshaler for MetadataSet.
// Accepts a flat JSON object: {"key": "str", "count": 42, "active": true, "cleared": null}
// Uses JSON type inference (see MetadataValueFromAny).
func (ms *MetadataSet) UnmarshalJSON(data []byte) error {
	var m map[string]any
	if err := jsonPkg.Unmarshal(data, &m); err != nil {
		return err
	}

	md, err := MetadataFromAnyMap(m)
	if err != nil {
		return err
	}

	ms.Metadata = md

	return nil
}

// AccountMetadataToAnyMap converts a map[string]*MetadataSet to map[string]map[string]any.
func AccountMetadataToAnyMap(m map[string]*MetadataSet) map[string]map[string]any {
	if m == nil {
		return nil
	}

	result := make(map[string]map[string]any, len(m))
	for k, v := range m {
		result[k] = MetadataSetToAnyMap(v)
	}

	return result
}
