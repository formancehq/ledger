package actions

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// StringMetadataFilter creates a filter matching a metadata string field with an exact value.
func StringMetadataFilter(key, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{
							Hardcoded: value,
						},
					},
				},
			},
		},
	}
}

// AddressPrefixFilter creates a filter matching accounts by address prefix.
func AddressPrefixFilter(prefix string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_HardcodedPrefix{
					HardcodedPrefix: prefix,
				},
			},
		},
	}
}

// AddressExactFilter creates a filter matching accounts by exact address.
func AddressExactFilter(addr string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_HardcodedExact{
					HardcodedExact: addr,
				},
			},
		},
	}
}

// ReferenceFilter creates a filter matching transactions by reference (exact match).
func ReferenceFilter(ref string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Reference{
			Reference: &commonpb.ReferenceCondition{
				Cond: &commonpb.StringCondition{
					Value: &commonpb.StringCondition_Hardcoded{
						Hardcoded: ref,
					},
				},
			},
		},
	}
}

// AndFilter creates a logical AND filter combining multiple filters.
func AndFilter(filters ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: filters},
		},
	}
}

// OrFilter creates a logical OR filter combining multiple filters.
func OrFilter(filters ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: filters},
		},
	}
}

// NotFilter creates a logical NOT filter.
func NotFilter(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Not{
			Not: &commonpb.NotFilter{Filter: f},
		},
	}
}

// LedgerFilter creates a filter matching entries by ledger name.
func LedgerFilter(ledger string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Ledger{
			Ledger: &commonpb.LedgerCondition{
				Cond: &commonpb.StringCondition{
					Value: &commonpb.StringCondition_Hardcoded{
						Hardcoded: ledger,
					},
				},
			},
		},
	}
}

// ParamAddressPrefixFilter creates a filter matching accounts by a parameterized address prefix.
// The actual prefix value is supplied at execution time via parameters map.
func ParamAddressPrefixFilter(paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_ParamPrefix{
					ParamPrefix: paramName,
				},
			},
		},
	}
}

// ParamAddressExactFilter creates a filter matching accounts by a parameterized exact address.
func ParamAddressExactFilter(paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_ParamExact{
					ParamExact: paramName,
				},
			},
		},
	}
}

// ParamStringMetadataFilter creates a filter matching a metadata string field with a parameterized value.
func ParamStringMetadataFilter(key, paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Param{
							Param: paramName,
						},
					},
				},
			},
		},
	}
}

// ParamBoolMetadataFilter creates a filter matching a metadata bool field with a parameterized value.
func ParamBoolMetadataFilter(key, paramName string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_BoolCond{
					BoolCond: &commonpb.BoolCondition{
						Value: &commonpb.BoolCondition_Param{
							Param: paramName,
						},
					},
				},
			},
		},
	}
}

// ParamInt64RangeMetadataFilter creates a filter matching a metadata int64 field
// with parameterized min/max bounds.
func ParamInt64RangeMetadataFilter(key, paramMin, paramMax string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_IntCond{
					IntCond: &commonpb.IntCondition{
						ParamMin: paramMin,
						ParamMax: paramMax,
					},
				},
			},
		},
	}
}

// Int64RangeMetadataFilter creates a filter matching a metadata int64 field
// with hardcoded min/max bounds (inclusive).
func Int64RangeMetadataFilter(key string, minVal, maxVal *int64) *commonpb.QueryFilter {
	return Int64RangeMetadataFilterExclusive(key, minVal, maxVal, false, false)
}

// Int64RangeMetadataFilterExclusive creates a filter matching a metadata int64 field
// with hardcoded min/max bounds and configurable exclusivity.
func Int64RangeMetadataFilterExclusive(key string, minVal, maxVal *int64, minExclusive, maxExclusive bool) *commonpb.QueryFilter {
	cond := &commonpb.IntCondition{
		MinExclusive: minExclusive,
		MaxExclusive: maxExclusive,
	}
	if minVal != nil {
		cond.Min = minVal
	}
	if maxVal != nil {
		cond.Max = maxVal
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_IntCond{
					IntCond: cond,
				},
			},
		},
	}
}

// UintMetadataFilter creates a filter matching a metadata uint64 field with
// a single exact value (closed range [val, val]).
func UintMetadataFilter(key string, val uint64) *commonpb.QueryFilter {
	v := val

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_UintCond{
					UintCond: &commonpb.UintCondition{
						Min: &v,
						Max: &v,
					},
				},
			},
		},
	}
}

// BoolMetadataFilter creates a filter matching a metadata bool field with a hardcoded value.
func BoolMetadataFilter(key string, val bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_BoolCond{
					BoolCond: &commonpb.BoolCondition{
						Value: &commonpb.BoolCondition_Hardcoded{
							Hardcoded: val,
						},
					},
				},
			},
		},
	}
}

// StringParam creates a ParameterValue with a string value.
func StringParam(s string) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: s}}
}

// Int64Param creates a ParameterValue with an int64 value.
func Int64Param(v int64) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Int64Value{Int64Value: v}}
}

// Uint64Param creates a ParameterValue with a uint64 value.
func Uint64Param(v uint64) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: v}}
}

// BoolParam creates a ParameterValue with a bool value.
func BoolParam(v bool) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_BoolValue{BoolValue: v}}
}

// ExistsMetadataFilter creates a filter that checks for metadata key existence.
func ExistsMetadataFilter(key string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
			},
		},
	}
}

// BuiltinUintRangeFilter creates a filter matching a builtin uint field within a range.
func BuiltinUintRangeFilter(field commonpb.TransactionBuiltinIndex, minVal, maxVal uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_BuiltinUint{
			BuiltinUint: &commonpb.BuiltinUintCondition{
				Field: field,
				Cond:  &commonpb.UintCondition{Min: &minVal, Max: &maxVal},
			},
		},
	}
}

// TimestampRangeFilter creates a filter matching transactions by timestamp range.
func TimestampRangeFilter(minVal, maxVal uint64) *commonpb.QueryFilter {
	return BuiltinUintRangeFilter(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, minVal, maxVal)
}

// InsertedAtRangeFilter creates a filter matching transactions by inserted-at range.
func InsertedAtRangeFilter(minVal, maxVal uint64) *commonpb.QueryFilter {
	return BuiltinUintRangeFilter(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, minVal, maxVal)
}

// RevertedAtRangeFilter creates a filter matching transactions by reverted-at range.
func RevertedAtRangeFilter(minVal, maxVal uint64) *commonpb.QueryFilter {
	return BuiltinUintRangeFilter(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT, minVal, maxVal)
}

// RevertedFilter creates a filter matching transactions by revert status.
func RevertedFilter(reverted bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Reverted{
			Reverted: &commonpb.RevertedCondition{Value: reverted},
		},
	}
}

// TxIDRangeFilter creates a filter matching transactions by ID range.
func TxIDRangeFilter(minVal, maxVal uint64) *commonpb.QueryFilter {
	return BuiltinUintRangeFilter(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, minVal, maxVal)
}

// TxIDExactFilter creates a filter matching a single transaction by exact ID.
func TxIDExactFilter(id uint64) *commonpb.QueryFilter {
	return TxIDRangeFilter(id, id)
}
