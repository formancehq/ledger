package commonpb

// Value is a stack-allocated metadata value union type that avoids heap
// allocations for numeric and boolean metadata. Unlike *MetadataValue (a proto
// pointer with an interface-based oneof), Value is a fixed-size value type
// that can be stored inline in maps and slices without pointer indirection.
//
// Use ToProto() and ValueFromProto() to convert at proto boundaries.
type Value struct {
	kind     valueKind
	intVal   int64
	uintVal  uint64
	boolVal  bool
	strVal   string
	original string // preserved for NullValue
}

type valueKind uint8

const (
	valueKindString valueKind = iota
	valueKindInt64
	valueKindUint64
	valueKindBool
	valueKindNull
)

// StringValue returns a Value holding a string.
func StringValue(s string) Value {
	return Value{kind: valueKindString, strVal: s}
}

// IntValue returns a Value holding an int64.
func IntValue(n int64) Value {
	return Value{kind: valueKindInt64, intVal: n}
}

// UintValue returns a Value holding a uint64.
func UintValue(n uint64) Value {
	return Value{kind: valueKindUint64, uintVal: n}
}

// BoolValue returns a Value holding a bool.
func BoolValue(b bool) Value {
	return Value{kind: valueKindBool, boolVal: b}
}

// NullValueOf returns a Value representing a null with the original string.
func NullValueOf(original string) Value {
	return Value{kind: valueKindNull, original: original}
}

// IsString returns true if this is a string value.
func (v Value) IsString() bool { return v.kind == valueKindString }

// IsInt returns true if this is an int64 value.
func (v Value) IsInt() bool { return v.kind == valueKindInt64 }

// IsUint returns true if this is a uint64 value.
func (v Value) IsUint() bool { return v.kind == valueKindUint64 }

// IsBool returns true if this is a bool value.
func (v Value) IsBool() bool { return v.kind == valueKindBool }

// IsNull returns true if this is a null value.
func (v Value) IsNull() bool { return v.kind == valueKindNull }

// AsString returns the string value. Only valid when IsString() is true.
func (v Value) AsString() string { return v.strVal }

// AsInt returns the int64 value. Only valid when IsInt() is true.
func (v Value) AsInt() int64 { return v.intVal }

// AsUint returns the uint64 value. Only valid when IsUint() is true.
func (v Value) AsUint() uint64 { return v.uintVal }

// AsBool returns the bool value. Only valid when IsBool() is true.
func (v Value) AsBool() bool { return v.boolVal }

// Original returns the original string for NullValue. Only valid when IsNull() is true.
func (v Value) Original() string { return v.original }

// ToProto converts this Value to a *MetadataValue proto message.
func (v Value) ToProto() *MetadataValue {
	switch v.kind {
	case valueKindString:
		return NewStringValue(v.strVal)
	case valueKindInt64:
		return NewIntValue(v.intVal)
	case valueKindUint64:
		return NewUintValue(v.uintVal)
	case valueKindBool:
		return NewBoolValue(v.boolVal)
	case valueKindNull:
		return NewNullValue(v.original)
	default:
		return NewNullValue("")
	}
}

// ValueFromProto converts a *MetadataValue proto message to a stack-allocated Value.
func ValueFromProto(v *MetadataValue) Value {
	if v == nil {
		return NullValueOf("")
	}

	switch t := v.GetType().(type) {
	case *MetadataValue_StringValue:
		return StringValue(t.StringValue)
	case *MetadataValue_IntValue:
		return IntValue(t.IntValue)
	case *MetadataValue_UintValue:
		return UintValue(t.UintValue)
	case *MetadataValue_BoolValue:
		return BoolValue(t.BoolValue)
	case *MetadataValue_NullValue:
		if t.NullValue != nil {
			return NullValueOf(t.NullValue.GetOriginal())
		}

		return NullValueOf("")
	default:
		return NullValueOf("")
	}
}
