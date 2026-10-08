package actions

import (
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// StringMetadataFilter creates a filter matching a metadata string field with an exact value.
func StringMetadataFilter(key, value string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_StringCond{
					StringCond: &ledgerpb.StringCondition{
						Value: &ledgerpb.StringCondition_Hardcoded{
							Hardcoded: value,
						},
					},
				},
			},
		},
	}
}

// AddressPrefixFilter creates a filter matching accounts by address prefix.
func AddressPrefixFilter(prefix string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Address{
			Address: &ledgerpb.AddressMatch{
				Match: &ledgerpb.AddressMatch_HardcodedPrefix{
					HardcodedPrefix: prefix,
				},
			},
		},
	}
}

// AddressExactFilter creates a filter matching accounts by exact address.
func AddressExactFilter(addr string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Address{
			Address: &ledgerpb.AddressMatch{
				Match: &ledgerpb.AddressMatch_HardcodedExact{
					HardcodedExact: addr,
				},
			},
		},
	}
}

// ReferenceFilter creates a filter matching transactions by reference (exact match).
func ReferenceFilter(ref string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Reference{
			Reference: &ledgerpb.ReferenceCondition{
				Cond: &ledgerpb.StringCondition{
					Value: &ledgerpb.StringCondition_Hardcoded{
						Hardcoded: ref,
					},
				},
			},
		},
	}
}

// AndFilter creates a logical AND filter combining multiple filters.
func AndFilter(filters ...*ledgerpb.QueryFilter) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_And{
			And: &ledgerpb.AndFilter{Filters: filters},
		},
	}
}

// OrFilter creates a logical OR filter combining multiple filters.
func OrFilter(filters ...*ledgerpb.QueryFilter) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Or{
			Or: &ledgerpb.OrFilter{Filters: filters},
		},
	}
}

// NotFilter creates a logical NOT filter.
func NotFilter(f *ledgerpb.QueryFilter) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Not{
			Not: &ledgerpb.NotFilter{Filter: f},
		},
	}
}

// LedgerFilter creates a filter matching entries by ledger name.
func LedgerFilter(ledger string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Ledger{
			Ledger: &ledgerpb.LedgerCondition{
				Cond: &ledgerpb.StringCondition{
					Value: &ledgerpb.StringCondition_Hardcoded{
						Hardcoded: ledger,
					},
				},
			},
		},
	}
}

// LogIdGreaterThanFilter creates a LOGS filter matching entries whose logId is
// strictly greater than ledgerLocalLogID.
//
// The bound is the per-ledger LedgerLog.Id (what LogIdCondition filters on), NOT
// the global audit/log sequence (Log.GetSequence()). The two coincide only while
// the global sequence has not yet diverged from ledger-local ids; once other
// ledgers (or ledger creation) advance the global sequence, passing
// Log.GetSequence() here skips too many rows. Always pass the ledger-local
// LedgerLog.Id.
func LogIdGreaterThanFilter(ledgerLocalLogID uint64) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_LogId{
			LogId: &ledgerpb.LogIdCondition{
				Cond: &ledgerpb.UintCondition{
					Min:          &ledgerLocalLogID,
					MinExclusive: true,
				},
			},
		},
	}
}

// ParamAddressPrefixFilter creates a filter matching accounts by a parameterized address prefix.
// The actual prefix value is supplied at execution time via parameters map.
func ParamAddressPrefixFilter(paramName string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Address{
			Address: &ledgerpb.AddressMatch{
				Match: &ledgerpb.AddressMatch_ParamPrefix{
					ParamPrefix: paramName,
				},
			},
		},
	}
}

// ParamAddressExactFilter creates a filter matching accounts by a parameterized exact address.
func ParamAddressExactFilter(paramName string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Address{
			Address: &ledgerpb.AddressMatch{
				Match: &ledgerpb.AddressMatch_ParamExact{
					ParamExact: paramName,
				},
			},
		},
	}
}

// ParamStringMetadataFilter creates a filter matching a metadata string field with a parameterized value.
func ParamStringMetadataFilter(key, paramName string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_StringCond{
					StringCond: &ledgerpb.StringCondition{
						Value: &ledgerpb.StringCondition_Param{
							Param: paramName,
						},
					},
				},
			},
		},
	}
}

// ParamBoolMetadataFilter creates a filter matching a metadata bool field with a parameterized value.
func ParamBoolMetadataFilter(key, paramName string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_BoolCond{
					BoolCond: &ledgerpb.BoolCondition{
						Value: &ledgerpb.BoolCondition_Param{
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
func ParamInt64RangeMetadataFilter(key, paramMin, paramMax string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_IntCond{
					IntCond: &ledgerpb.IntCondition{
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
func Int64RangeMetadataFilter(key string, minVal, maxVal *int64) *ledgerpb.QueryFilter {
	return Int64RangeMetadataFilterExclusive(key, minVal, maxVal, false, false)
}

// Int64RangeMetadataFilterExclusive creates a filter matching a metadata int64 field
// with hardcoded min/max bounds and configurable exclusivity.
func Int64RangeMetadataFilterExclusive(key string, minVal, maxVal *int64, minExclusive, maxExclusive bool) *ledgerpb.QueryFilter {
	cond := &ledgerpb.IntCondition{
		MinExclusive: minExclusive,
		MaxExclusive: maxExclusive,
	}
	if minVal != nil {
		cond.Min = minVal
	}
	if maxVal != nil {
		cond.Max = maxVal
	}

	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_IntCond{
					IntCond: cond,
				},
			},
		},
	}
}

// UintMetadataFilter creates a filter matching a metadata uint64 field with
// a single exact value (closed range [val, val]).
func UintMetadataFilter(key string, val uint64) *ledgerpb.QueryFilter {
	v := val

	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_UintCond{
					UintCond: &ledgerpb.UintCondition{
						Min: &v,
						Max: &v,
					},
				},
			},
		},
	}
}

// BoolMetadataFilter creates a filter matching a metadata bool field with a hardcoded value.
func BoolMetadataFilter(key string, val bool) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_BoolCond{
					BoolCond: &ledgerpb.BoolCondition{
						Value: &ledgerpb.BoolCondition_Hardcoded{
							Hardcoded: val,
						},
					},
				},
			},
		},
	}
}

// StringParam creates a ParameterValue with a string value.
func StringParam(s string) *ledgerpb.ParameterValue {
	return &ledgerpb.ParameterValue{Value: &ledgerpb.ParameterValue_StringValue{StringValue: s}}
}

// Int64Param creates a ParameterValue with an int64 value.
func Int64Param(v int64) *ledgerpb.ParameterValue {
	return &ledgerpb.ParameterValue{Value: &ledgerpb.ParameterValue_Int64Value{Int64Value: v}}
}

// Uint64Param creates a ParameterValue with a uint64 value.
func Uint64Param(v uint64) *ledgerpb.ParameterValue {
	return &ledgerpb.ParameterValue{Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: v}}
}

// BoolParam creates a ParameterValue with a bool value.
func BoolParam(v bool) *ledgerpb.ParameterValue {
	return &ledgerpb.ParameterValue{Value: &ledgerpb.ParameterValue_BoolValue{BoolValue: v}}
}

// ExistsMetadataFilter creates a filter that checks for metadata key existence.
func ExistsMetadataFilter(key string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field:     &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_ExistsCond{ExistsCond: &ledgerpb.ExistsCondition{}},
			},
		},
	}
}

// BuiltinUintRangeFilter creates a filter matching a builtin uint field within a range.
func BuiltinUintRangeFilter(field ledgerpb.TransactionBuiltinIndex, minVal, maxVal uint64) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_BuiltinUint{
			BuiltinUint: &ledgerpb.BuiltinUintCondition{
				Field: field,
				Cond:  &ledgerpb.UintCondition{Min: &minVal, Max: &maxVal},
			},
		},
	}
}

// TimestampRangeFilter creates a filter matching transactions by timestamp range.
func TimestampRangeFilter(minVal, maxVal uint64) *ledgerpb.QueryFilter {
	return BuiltinUintRangeFilter(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, minVal, maxVal)
}

// InsertedAtRangeFilter creates a filter matching transactions by inserted-at range.
func InsertedAtRangeFilter(minVal, maxVal uint64) *ledgerpb.QueryFilter {
	return BuiltinUintRangeFilter(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, minVal, maxVal)
}

// RevertedAtRangeFilter creates a filter matching transactions by reverted-at range.
func RevertedAtRangeFilter(minVal, maxVal uint64) *ledgerpb.QueryFilter {
	return BuiltinUintRangeFilter(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT, minVal, maxVal)
}

// RevertedFilter creates a filter matching transactions by revert status.
func RevertedFilter(reverted bool) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Reverted{
			Reverted: &ledgerpb.RevertedCondition{Value: reverted},
		},
	}
}

// TxIDRangeFilter creates a filter matching transactions by ID range.
func TxIDRangeFilter(minVal, maxVal uint64) *ledgerpb.QueryFilter {
	return BuiltinUintRangeFilter(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, minVal, maxVal)
}

// TxIDExactFilter creates a filter matching a single transaction by exact ID.
func TxIDExactFilter(id uint64) *ledgerpb.QueryFilter {
	return TxIDRangeFilter(id, id)
}

// AddressExactRoleFilter creates a filter matching transactions whose posting
// has addr in the given role (source, destination, or either).
func AddressExactRoleFilter(addr string, role ledgerpb.AddressRole) *ledgerpb.QueryFilter {
	f := AddressExactFilter(addr)
	f.GetAddress().Role = role

	return f
}
