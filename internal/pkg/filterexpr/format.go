package filterexpr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Precedence levels for parenthesization.
const (
	precOr   = 1
	precAnd  = 2
	precNot  = 3
	precLeaf = 4
)

// Format converts a QueryFilter proto message back to the human-readable DSL
// string. This is the inverse of Parse.
func Format(f *commonpb.QueryFilter) string {
	if f == nil {
		return ""
	}
	s, _ := formatFilter(f)

	return s
}

// formatFilter returns the formatted string and the precedence level of the
// expression, so callers can decide whether to wrap in parentheses.
func formatFilter(f *commonpb.QueryFilter) (string, int) {
	switch v := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Field:
		return formatFieldCondition(v.Field), precLeaf
	case *commonpb.QueryFilter_Address:
		return formatAddressMatch(v.Address), precLeaf
	case *commonpb.QueryFilter_And:
		return formatBinaryOp(v.And.GetFilters(), "and", precAnd), precAnd
	case *commonpb.QueryFilter_Or:
		return formatBinaryOp(v.Or.GetFilters(), "or", precOr), precOr
	case *commonpb.QueryFilter_Not:
		return formatNot(v.Not)
	default:
		return "<unknown filter>", precLeaf
	}
}

// formatWithPrec formats a child filter, wrapping it in parentheses if its
// precedence is lower than the parent's.
func formatWithPrec(f *commonpb.QueryFilter, parentPrec int) string {
	s, prec := formatFilter(f)
	if prec < parentPrec {
		return "(" + s + ")"
	}

	return s
}

func formatBinaryOp(filters []*commonpb.QueryFilter, op string, prec int) string {
	parts := make([]string, len(filters))
	for i, f := range filters {
		parts[i] = formatWithPrec(f, prec)
	}

	return strings.Join(parts, " "+op+" ")
}

func formatNot(n *commonpb.NotFilter) (string, int) {
	// Sugar: not(field == val) → metadata[key] != val
	if fc := n.GetFilter().GetField(); fc != nil {
		if ne := formatAsNotEqual(fc); ne != "" {
			return ne, precLeaf
		}
	}

	return "not " + formatWithPrec(n.GetFilter(), precNot), precNot
}

// formatAsNotEqual tries to render a FieldCondition wrapped in NOT as a != expression.
// Returns empty string if the condition is not a simple equality.
func formatAsNotEqual(fc *commonpb.FieldCondition) string {
	key := fc.GetField().GetMetadata()
	switch cond := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		return fmt.Sprintf("metadata[%s] != %s", key, formatStringCondValue(cond.StringCond))
	case *commonpb.FieldCondition_IntCond:
		if eq := formatIntCondAsEquality(cond.IntCond); eq != "" {
			return fmt.Sprintf("metadata[%s] != %s", key, eq)
		}
	case *commonpb.FieldCondition_BoolCond:
		return fmt.Sprintf("metadata[%s] != %s", key, formatBoolCondValue(cond.BoolCond))
	}

	return ""
}

func formatFieldCondition(fc *commonpb.FieldCondition) string {
	key := fc.GetField().GetMetadata()

	switch cond := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		return fmt.Sprintf("metadata[%s] == %s", key, formatStringCondValue(cond.StringCond))
	case *commonpb.FieldCondition_IntCond:
		return formatIntCondition(key, cond.IntCond)
	case *commonpb.FieldCondition_UintCond:
		return formatUintCondition(key, cond.UintCond)
	case *commonpb.FieldCondition_BoolCond:
		return fmt.Sprintf("metadata[%s] == %s", key, formatBoolCondValue(cond.BoolCond))
	case *commonpb.FieldCondition_ExistsCond:
		return fmt.Sprintf("metadata[%s] exists", key)
	default:
		return fmt.Sprintf("metadata[%s] <unknown>", key)
	}
}

func formatStringCondValue(sc *commonpb.StringCondition) string {
	switch v := sc.GetValue().(type) {
	case *commonpb.StringCondition_Param:
		return "$" + v.Param
	case *commonpb.StringCondition_Hardcoded:
		return quoteIfNeeded(v.Hardcoded)
	default:
		return `""`
	}
}

func formatBoolCondValue(bc *commonpb.BoolCondition) string {
	switch v := bc.GetValue().(type) {
	case *commonpb.BoolCondition_Param:
		return "$" + v.Param
	case *commonpb.BoolCondition_Hardcoded:
		if v.Hardcoded {
			return "true"
		}

		return "false"
	default:
		return "false"
	}
}

// formatIntCondAsEquality returns the value string if the IntCondition represents
// an exact equality (min == max, both non-nil, no exclusion). Returns "" otherwise.
func formatIntCondAsEquality(ic *commonpb.IntCondition) string {
	if ic.Min != nil && ic.Max != nil && ic.GetMin() == ic.GetMax() && !ic.GetMinExclusive() && !ic.GetMaxExclusive() {
		return strconv.FormatInt(ic.GetMin(), 10)
	}

	return ""
}

func formatIntCondition(key string, ic *commonpb.IntCondition) string {
	// Equality: min == max, both set, no exclusion
	if eq := formatIntCondAsEquality(ic); eq != "" {
		return fmt.Sprintf("metadata[%s] == %s", key, eq)
	}

	hasLow := ic.Min != nil || ic.GetParamMin() != ""
	hasHigh := ic.Max != nil || ic.GetParamMax() != ""

	// Bounded range (both ends present) → "between LOW and HIGH" (inclusive).
	// Exclusivity on hardcoded bounds is normalized by adjusting the value
	// (int64 is discrete). Param bounds carry no exclusivity in the DSL.
	if hasLow && hasHigh {
		return fmt.Sprintf("metadata[%s] between %s and %s",
			key, formatIntLowInclusive(ic), formatIntHighInclusive(ic))
	}

	// Single bound: keep the original operator so `> 18` round-trips as `> 18`,
	// not as `>= 19`. Fidelity matters more than canonicalization here.
	if hasLow {
		if ic.GetParamMin() != "" {
			op := ">="
			if ic.GetMinExclusive() {
				op = ">"
			}

			return fmt.Sprintf("metadata[%s] %s $%s", key, op, ic.GetParamMin())
		}

		op := ">="
		if ic.GetMinExclusive() {
			op = ">"
		}

		return fmt.Sprintf("metadata[%s] %s %d", key, op, ic.GetMin())
	}
	if hasHigh {
		if ic.GetParamMax() != "" {
			op := "<="
			if ic.GetMaxExclusive() {
				op = "<"
			}

			return fmt.Sprintf("metadata[%s] %s $%s", key, op, ic.GetParamMax())
		}

		op := "<="
		if ic.GetMaxExclusive() {
			op = "<"
		}

		return fmt.Sprintf("metadata[%s] %s %d", key, op, ic.GetMax())
	}

	return fmt.Sprintf("metadata[%s] <int?>", key)
}

// formatIntLowInclusive renders the lower bound for `between` output, with
// any MinExclusive flag normalized away by incrementing the literal value.
// Caller must have verified the bound is present.
func formatIntLowInclusive(ic *commonpb.IntCondition) string {
	if ic.GetParamMin() != "" {
		return "$" + ic.GetParamMin()
	}

	v := ic.GetMin()
	if ic.GetMinExclusive() {
		v++
	}

	return strconv.FormatInt(v, 10)
}

// formatIntHighInclusive is the symmetric helper for the upper bound.
func formatIntHighInclusive(ic *commonpb.IntCondition) string {
	if ic.GetParamMax() != "" {
		return "$" + ic.GetParamMax()
	}

	v := ic.GetMax()
	if ic.GetMaxExclusive() {
		v--
	}

	return strconv.FormatInt(v, 10)
}

func formatUintCondition(key string, uc *commonpb.UintCondition) string {
	// Equality
	if uc.Min != nil && uc.Max != nil && uc.GetMin() == uc.GetMax() && !uc.GetMinExclusive() && !uc.GetMaxExclusive() {
		return fmt.Sprintf("metadata[%s] == %d", key, uc.GetMin())
	}

	hasLow := uc.Min != nil || uc.GetParamMin() != ""
	hasHigh := uc.Max != nil || uc.GetParamMax() != ""

	if hasLow && hasHigh {
		return fmt.Sprintf("metadata[%s] between %s and %s",
			key, formatUintLowInclusive(uc), formatUintHighInclusive(uc))
	}

	if hasLow {
		if uc.GetParamMin() != "" {
			op := ">="
			if uc.GetMinExclusive() {
				op = ">"
			}

			return fmt.Sprintf("metadata[%s] %s $%s", key, op, uc.GetParamMin())
		}

		op := ">="
		if uc.GetMinExclusive() {
			op = ">"
		}

		return fmt.Sprintf("metadata[%s] %s %d", key, op, uc.GetMin())
	}
	if hasHigh {
		if uc.GetParamMax() != "" {
			op := "<="
			if uc.GetMaxExclusive() {
				op = "<"
			}

			return fmt.Sprintf("metadata[%s] %s $%s", key, op, uc.GetParamMax())
		}

		op := "<="
		if uc.GetMaxExclusive() {
			op = "<"
		}

		return fmt.Sprintf("metadata[%s] %s %d", key, op, uc.GetMax())
	}

	return fmt.Sprintf("metadata[%s] <uint?>", key)
}

func formatUintLowInclusive(uc *commonpb.UintCondition) string {
	if uc.GetParamMin() != "" {
		return "$" + uc.GetParamMin()
	}

	v := uc.GetMin()
	if uc.GetMinExclusive() {
		v++
	}

	return strconv.FormatUint(v, 10)
}

func formatUintHighInclusive(uc *commonpb.UintCondition) string {
	if uc.GetParamMax() != "" {
		return "$" + uc.GetParamMax()
	}

	v := uc.GetMax()
	if uc.GetMaxExclusive() {
		v--
	}

	return strconv.FormatUint(v, 10)
}

func formatAddressMatch(am *commonpb.AddressMatch) string {
	keyword := "address"
	switch am.GetRole() {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		keyword = "source"
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		keyword = "destination"
	}

	switch v := am.GetMatch().(type) {
	case *commonpb.AddressMatch_HardcodedExact:
		return fmt.Sprintf("%s == %s", keyword, quoteIfNeeded(v.HardcodedExact))
	case *commonpb.AddressMatch_HardcodedPrefix:
		return fmt.Sprintf("%s ^= %s", keyword, quoteIfNeeded(v.HardcodedPrefix))
	case *commonpb.AddressMatch_ParamExact:
		return fmt.Sprintf("%s == $%s", keyword, v.ParamExact)
	case *commonpb.AddressMatch_ParamPrefix:
		return fmt.Sprintf("%s ^= $%s", keyword, v.ParamPrefix)
	default:
		return keyword + " <unknown>"
	}
}

// quoteIfNeeded wraps a value in double quotes if it contains spaces or matches
// a DSL keyword. Simple identifiers are left bare.
func quoteIfNeeded(s string) string {
	if s == "" {
		return `""`
	}
	if needsQuoting(s) {
		return `"` + s + `"`
	}

	return s
}

var keywords = map[string]bool{
	"and": true, "or": true, "not": true, "between": true,
	"metadata": true, "address": true, "source": true, "destination": true,
	"exists": true, "true": true, "false": true,
}

func needsQuoting(s string) bool {
	if keywords[s] {
		return true
	}
	for _, c := range s {
		if c == ' ' || c == '\t' || c == '"' || c == '\'' || c == '(' || c == ')' ||
			c == '[' || c == ']' {
			return true
		}
	}

	return false
}
