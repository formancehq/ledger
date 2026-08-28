package main

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Metadata Field filters. The compiler serves a Field condition from the
// per-(target, key) metadata index: the key must be DECLARED in the target's
// schema (else index-not-found), the index must be READY, and the condition
// kind must be compatible with the declared type (validateAndCoerceCondition —
// else FILTER_COMPILATION_ERROR). The index holds each entity's CURRENT value
// coerced to the declared type (CoerceToDeclaredType at write, replace
// semantics, deletes remove rows), so evaluation coerces the stored value the
// same way and compares under the condition kind.

// --- constructors ----------------------------------------------------------

func fieldRef(key string) *commonpb.FieldRef {
	return &commonpb.FieldRef{Metadata: key}
}

func filterFieldString(key, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field: fieldRef(key),
		Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
		}},
	}}}
}

// filterFieldInt matches key values in [lo, hi], both bounds inclusive; nil
// leaves a side open.
func filterFieldInt(key string, lo, hi *int64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     fieldRef(key),
		Condition: &commonpb.FieldCondition_IntCond{IntCond: &commonpb.IntCondition{Min: lo, Max: hi}},
	}}}
}

// filterFieldUint matches key values in [lo, hi], both bounds inclusive; nil
// leaves a side open.
func filterFieldUint(key string, lo, hi *uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     fieldRef(key),
		Condition: &commonpb.FieldCondition_UintCond{UintCond: &commonpb.UintCondition{Min: lo, Max: hi}},
	}}}
}

func filterFieldBool(key string, value bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field: fieldRef(key),
		Condition: &commonpb.FieldCondition_BoolCond{BoolCond: &commonpb.BoolCondition{
			Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: value},
		}},
	}}}
}

func filterFieldExists(key string, includeNull bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     fieldRef(key),
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{IncludeNull: includeNull}},
	}}}
}

// --- evaluation ------------------------------------------------------------

// matchFieldCondition evaluates a Field leaf against an entity's stored
// metadata under the declared types. The index stores the value coerced to the
// declared type, so the model coerces identically (the server's own
// ConvertMetadataValue) before comparing. An undeclared key or a missing value
// has no index row and never matches.
func matchFieldCondition(declared oracle.Map[string, commonpb.MetadataType], lookup func(string) (*commonpb.MetadataValue, bool), fc *commonpb.FieldCondition) bool {
	key := fc.GetField().GetMetadata()

	declaredType, ok := declared.Get(key)
	if !ok {
		return false
	}

	stored, present := lookup(key)
	if !present {
		return false
	}

	coerced := stored
	if !commonpb.TypeMatches(stored, declaredType) {
		coerced = commonpb.ConvertMetadataValue(stored, declaredType)
	}

	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_ExistsCond:
		if c.ExistsCond.GetIncludeNull() {
			return true
		}
		_, isNull := coerced.GetType().(*commonpb.MetadataValue_NullValue)

		return !isNull
	case *commonpb.FieldCondition_StringCond:
		sv, isStr := coerced.GetType().(*commonpb.MetadataValue_StringValue)

		return isStr && sv.StringValue == c.StringCond.GetHardcoded()
	case *commonpb.FieldCondition_BoolCond:
		bv, isBool := coerced.GetType().(*commonpb.MetadataValue_BoolValue)

		return isBool && bv.BoolValue == c.BoolCond.GetHardcoded()
	case *commonpb.FieldCondition_IntCond:
		// Datetime shares the order-preserving int64 index encoding, so an int
		// range scans datetime rows too (validateAndCoerceCondition admits it).
		// On an unsigned-declared field the compiler coerces the int bounds
		// into a uint condition (coerceIntToUint), so a UintValue matches by
		// the same numeric bounds; negative bounds never reach here — they are
		// a compilation rejection (fieldKindMismatch).
		switch t := coerced.GetType().(type) {
		case *commonpb.MetadataValue_IntValue:
			return matchInt64Bounds(c.IntCond, t.IntValue)
		case *commonpb.MetadataValue_DatetimeValue:
			return matchInt64Bounds(c.IntCond, t.DatetimeValue)
		case *commonpb.MetadataValue_UintValue:
			uc, ok := intCondAsUint(c.IntCond)

			return ok && matchTxIDBounds(uc, t.UintValue)
		default:
			return false
		}
	case *commonpb.FieldCondition_UintCond:
		uv, isUint := coerced.GetType().(*commonpb.MetadataValue_UintValue)

		return isUint && matchTxIDBounds(c.UintCond, uv.UintValue)
	default:
		return false
	}
}

// intCondAsUint mirrors the compiler's coerceIntToUint: the int bounds carried
// into the unsigned domain, ok=false on a negative bound (the compiler rejects
// those, so a match verdict is never reached).
func intCondAsUint(cond *commonpb.IntCondition) (*commonpb.UintCondition, bool) {
	uc := &commonpb.UintCondition{
		MinExclusive: cond.GetMinExclusive(),
		MaxExclusive: cond.GetMaxExclusive(),
	}

	if cond.Min != nil {
		if cond.GetMin() < 0 {
			return nil, false
		}

		v := uint64(cond.GetMin())
		uc.Min = &v
	}

	if cond.Max != nil {
		if cond.GetMax() < 0 {
			return nil, false
		}

		v := uint64(cond.GetMax())
		uc.Max = &v
	}

	return uc, true
}

// matchInt64Bounds is the signed twin of matchTxIDBounds: min/max honor their
// exclusive flags, an absent bound is open on that side.
func matchInt64Bounds(cond *commonpb.IntCondition, n int64) bool {
	if cond.Min != nil {
		if cond.GetMinExclusive() {
			if n <= cond.GetMin() {
				return false
			}
		} else if n < cond.GetMin() {
			return false
		}
	}

	if cond.Max != nil {
		if cond.GetMaxExclusive() {
			if n >= cond.GetMax() {
				return false
			}
		} else if n > cond.GetMax() {
			return false
		}
	}

	return true
}

// fieldKindMismatch reports whether f contains a Field leaf whose condition
// kind is incompatible with the key's declared type — the shapes
// validateAndCoerceCondition rejects with FILTER_COMPILATION_ERROR
// (InvalidArgument). Undeclared keys are NOT mismatches: the compiler rejects
// them earlier as index-not-found. The compiler only reaches the coercion
// check after requireIndexReady, so a mismatch verdict competes with the
// not-ready rejection — validateFieldMismatchProbe accepts either.
func fieldKindMismatch(ls oracle.LedgerState, f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	if f == nil {
		return false
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			if fieldKindMismatch(ls, child, target) {
				return true
			}
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			if fieldKindMismatch(ls, child, target) {
				return true
			}
		}
	case *commonpb.QueryFilter_Not:
		return fieldKindMismatch(ls, x.Not.GetFilter(), target)
	case *commonpb.QueryFilter_Field:
		declaredType, ok := declaredFieldTypes(ls, target).Get(x.Field.GetField().GetMetadata())
		if !ok {
			return false
		}

		switch cond := x.Field.GetCondition().(type) {
		case *commonpb.FieldCondition_ExistsCond:
			return false
		case *commonpb.FieldCondition_IntCond:
			// Signed and datetime verbatim; unsigned via coerceIntToUint,
			// which rejects negative bounds.
			if commonpb.IsSignedType(declaredType) || commonpb.IsDatetimeType(declaredType) {
				return false
			}

			if commonpb.IsUnsignedType(declaredType) {
				return (cond.IntCond.Min != nil && cond.IntCond.GetMin() < 0) ||
					(cond.IntCond.Max != nil && cond.IntCond.GetMax() < 0)
			}

			return true
		case *commonpb.FieldCondition_UintCond:
			return !commonpb.IsUnsignedType(declaredType)
		case *commonpb.FieldCondition_StringCond:
			return declaredType != commonpb.MetadataType_METADATA_TYPE_STRING
		case *commonpb.FieldCondition_BoolCond:
			return declaredType != commonpb.MetadataType_METADATA_TYPE_BOOL
		}
	}

	return false
}

// declaredFieldTypes returns the declared-type map matching a query target.
func declaredFieldTypes(ls oracle.LedgerState, target commonpb.QueryTarget) oracle.Map[string, commonpb.MetadataType] {
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		return ls.TransactionFieldTypes()
	}

	return ls.AccountFieldTypes()
}

// --- generation ------------------------------------------------------------

// fieldSeed is one declared (key, type) with an optional committed value the
// generator can aim a hit at.
type fieldSeed struct {
	key          string
	declaredType commonpb.MetadataType
	sample       *commonpb.MetadataValue
}

// sampleFieldSeeds snapshots the declared fields of the target plus one stored
// value per key (coerced-comparable as-is; the generator coerces itself).
// Caller holds c.mu (like the other committed-state samplers).
func sampleFieldSeeds(ls oracle.LedgerState, target commonpb.QueryTarget) []fieldSeed {
	var seeds []fieldSeed
	for key, dt := range declaredFieldTypes(ls, target).All() {
		seed := fieldSeed{key: key, declaredType: dt}

		if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
			for _, rec := range ls.Txs().All() {
				if v, ok := rec.Metadata()[key]; ok {
					seed.sample = v

					break
				}
			}
		} else {
			for mk, v := range ls.Metadata().All() {
				if mk.Key == key {
					seed.sample = v

					break
				}
			}
		}

		seeds = append(seeds, seed)
	}

	return seeds
}

// genFieldLeaf rolls a kind-compatible Field leaf on a declared key, aiming
// bounds/values at a committed sample when one exists so the filter actually
// straddles live rows. One-in-eight it deliberately rolls a kind-MISMATCHED
// leaf (the FILTER_COMPILATION_ERROR probe). Returns nil when the target has
// no declared fields.
func genFieldLeaf(seeds []fieldSeed) *commonpb.QueryFilter {
	if len(seeds) == 0 {
		return nil
	}

	seed := seeds[internal.Rand().Intn(len(seeds))]

	// Coerce the sample to the declared type — the value space the index holds.
	var coerced *commonpb.MetadataValue
	if seed.sample != nil {
		coerced = seed.sample
		if !commonpb.TypeMatches(coerced, seed.declaredType) {
			coerced = commonpb.ConvertMetadataValue(coerced, seed.declaredType)
		}
	}

	if oneIn(6) {
		return filterFieldExists(seed.key, oneIn(2))
	}

	switch {
	case seed.declaredType == commonpb.MetadataType_METADATA_TYPE_STRING:
		if sv, ok := coerced.GetType().(*commonpb.MetadataValue_StringValue); ok && !oneIn(4) {
			return filterFieldString(seed.key, sv.StringValue)
		}

		return filterFieldString(seed.key, "absent-value")
	case seed.declaredType == commonpb.MetadataType_METADATA_TYPE_BOOL:
		return filterFieldBool(seed.key, oneIn(2))
	case commonpb.IsUnsignedType(seed.declaredType):
		center := internal.Rand().Uint64() % 1024
		if uv, ok := coerced.GetType().(*commonpb.MetadataValue_UintValue); ok && !oneIn(4) {
			center = uv.UintValue
		}

		return filterFieldUint(seed.key, uintBound(center, true), uintBound(center, false))
	default:
		// Signed and datetime both take int bounds.
		var center int64
		switch t := coerced.GetType().(type) {
		case *commonpb.MetadataValue_IntValue:
			center = t.IntValue
		case *commonpb.MetadataValue_DatetimeValue:
			center = t.DatetimeValue
		default:
			center = internal.Rand().Int63n(1024)
		}

		return filterFieldInt(seed.key, intBound(center, true), intBound(center, false))
	}
}

// genMismatchedFieldLeaf rolls a bare Field leaf whose condition kind the
// declared type rejects — the FILTER_COMPILATION_ERROR probe. Emitted only at
// the top level so the rejection attribution stays unambiguous. Returns nil
// when the target declares no fields.
func genMismatchedFieldLeaf(seeds []fieldSeed) *commonpb.QueryFilter {
	if len(seeds) == 0 {
		return nil
	}

	seed := seeds[internal.Rand().Intn(len(seeds))]

	switch {
	case seed.declaredType == commonpb.MetadataType_METADATA_TYPE_STRING:
		return filterFieldBool(seed.key, true)
	case seed.declaredType == commonpb.MetadataType_METADATA_TYPE_BOOL:
		lo := int64(0)

		return filterFieldInt(seed.key, &lo, nil)
	case commonpb.IsUnsignedType(seed.declaredType):
		return filterFieldString(seed.key, "x")
	default:
		return filterFieldString(seed.key, "x")
	}
}

// intBound rolls an inclusive bound around center: lower ≤ center for lo,
// ≥ center for hi, occasionally open (nil).
func intBound(center int64, lo bool) *int64 {
	if oneIn(4) {
		return nil
	}

	delta := internal.Rand().Int63n(64)
	v := center + delta
	if lo {
		v = center - delta
	}

	return &v
}

func uintBound(center uint64, lo bool) *uint64 {
	if oneIn(4) {
		return nil
	}

	delta := internal.Rand().Uint64() % 64
	v := center + delta
	if lo {
		if delta > center {
			delta = center
		}
		v = center - delta
	}

	return &v
}
