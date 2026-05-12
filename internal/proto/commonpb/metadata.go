package commonpb

import (
	"fmt"
	"math"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	jsonPkg "github.com/formancehq/ledger-v3-poc/internal/adapter/json"
)

// Target type constants.
const (
	MetaTargetTypeAccount     = "ACCOUNT"
	MetaTargetTypeTransaction = "TRANSACTION"
)

// MetadataFromGoMap converts a metadata.Metadata (map[string]string) to a map[string]*MetadataValue.
func MetadataFromGoMap(m metadata.Metadata) map[string]*MetadataValue {
	if m == nil {
		return nil
	}

	result := make(map[string]*MetadataValue, len(m))
	for k, v := range m {
		result[k] = NewStringValue(v)
	}

	return result
}

// MetadataToGoMap converts a map[string]*MetadataValue to metadata.Metadata (map[string]string).
func MetadataToGoMap(m map[string]*MetadataValue) metadata.Metadata {
	if m == nil {
		return nil
	}

	result := make(metadata.Metadata, len(m))
	for k, v := range m {
		if v != nil {
			result[k] = MetadataValueToString(v)
		}
	}

	return result
}

// MetadataMapToGoMap converts a *MetadataMap to metadata.Metadata (map[string]string).
func MetadataMapToGoMap(mm *MetadataMap) metadata.Metadata {
	if mm == nil {
		return nil
	}

	return MetadataToGoMap(mm.GetValues())
}

// MetadataMapFromGoMap converts a metadata.Metadata (map[string]string) to a *MetadataMap.
func MetadataMapFromGoMap(m metadata.Metadata) *MetadataMap {
	if m == nil {
		return nil
	}

	return &MetadataMap{
		Values: MetadataFromGoMap(m),
	}
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

// MetadataToAnyMap converts a map[string]*MetadataValue to map[string]any with typed values.
func MetadataToAnyMap(m map[string]*MetadataValue) map[string]any {
	if m == nil {
		return nil
	}

	result := make(map[string]any, len(m))
	for k, v := range m {
		if v != nil {
			result[k] = MetadataValueToAny(v)
		}
	}

	return result
}

// MetadataMapToAnyMap converts a *MetadataMap to map[string]any with typed values.
func MetadataMapToAnyMap(mm *MetadataMap) map[string]any {
	if mm == nil {
		return nil
	}

	return MetadataToAnyMap(mm.GetValues())
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

// MetadataFromAnyMap converts a map[string]any to map[string]*MetadataValue with JSON type inference.
// Keys with nil values are skipped (nil signals deletion at the HTTP layer).
func MetadataFromAnyMap(m map[string]any) (map[string]*MetadataValue, error) {
	if m == nil {
		return nil, nil
	}

	result := make(map[string]*MetadataValue, len(m))
	for k, v := range m {
		mv, err := MetadataValueFromAny(v)
		if err != nil {
			return nil, fmt.Errorf("metadata key %q: %w", k, err)
		}

		if mv == nil {
			continue // nil means delete this key
		}

		result[k] = mv
	}

	return result, nil
}

// AccountMetadataToAnyMap converts a map[string]*MetadataMap to map[string]map[string]any.
func AccountMetadataToAnyMap(m map[string]*MetadataMap) map[string]map[string]any {
	if m == nil {
		return nil
	}

	result := make(map[string]map[string]any, len(m))
	for k, v := range m {
		result[k] = MetadataMapToAnyMap(v)
	}

	return result
}

// MarshalJSON implements json.Marshaler for MetadataMap.
// Outputs a flat JSON object with typed values: {"key": "str", "count": 42, "active": true}.
func (mm *MetadataMap) MarshalJSON() ([]byte, error) {
	m := MetadataMapToAnyMap(mm)
	if m == nil {
		m = make(map[string]any)
	}

	return jsonPkg.Marshal(m)
}

// UnmarshalJSON implements json.Unmarshaler for MetadataMap.
// Accepts a flat JSON object: {"key": "str", "count": 42, "active": true, "cleared": null}
// Uses JSON type inference (see MetadataValueFromAny).
func (mm *MetadataMap) UnmarshalJSON(data []byte) error {
	var m map[string]any
	if err := jsonPkg.Unmarshal(data, &m); err != nil {
		return err
	}

	md, err := MetadataFromAnyMap(m)
	if err != nil {
		return err
	}

	mm.Values = md

	return nil
}
