package commonpb

import (
	"regexp"
	"slices"
)

// FilterGrammarKeywords are the reserved words of the textual filter grammar
// (internal/pkg/filterexpr builds its Keyword token from this list). A
// keyword lexes as Keyword, never Ident, so `$and` is not a parameter
// reference in the textual form.
var FilterGrammarKeywords = []string{
	"and", "or", "not", "in", "between", "metadata", "address", "source",
	"destination", "ledger", "exists", "true", "false",
}

var filterParamNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// IsTextualFilterParamName reports whether name renders as `$name` and parses
// back as the same parameter reference in the textual filter grammar: an Ident
// that is not a keyword. The JSON DSL accepts any string after "$param", so a
// name outside this set would export to text that re-parses as different
// conditions.
func IsTextualFilterParamName(name string) bool {
	if !filterParamNamePattern.MatchString(name) {
		return false
	}

	return !slices.Contains(FilterGrammarKeywords, name)
}

// FilterLeafParamNames returns the parameter names a leaf condition
// references, across every condition kind that can carry one. A param oneof
// arm that is set (string, bool and address conditions) is reported even when
// its name is empty, so a caller validating names sees it; the int/uint
// ParamMin/ParamMax fields are plain strings where empty means "no param".
// Combinators return nothing; callers walk the tree.
func FilterLeafParamNames(f *QueryFilter) []string {
	var names []string

	addRange := func(lo, hi string) {
		for _, name := range []string{lo, hi} {
			if name != "" {
				names = append(names, name)
			}
		}
	}
	addString := func(sc *StringCondition) {
		if v, ok := sc.GetValue().(*StringCondition_Param); ok {
			names = append(names, v.Param)
		}
	}

	switch v := f.GetFilter().(type) {
	case *QueryFilter_Field:
		switch c := v.Field.GetCondition().(type) {
		case *FieldCondition_StringCond:
			addString(c.StringCond)
		case *FieldCondition_BoolCond:
			if p, ok := c.BoolCond.GetValue().(*BoolCondition_Param); ok {
				names = append(names, p.Param)
			}
		case *FieldCondition_IntCond:
			addRange(c.IntCond.GetParamMin(), c.IntCond.GetParamMax())
		case *FieldCondition_UintCond:
			addRange(c.UintCond.GetParamMin(), c.UintCond.GetParamMax())
		}
	case *QueryFilter_Address:
		switch m := v.Address.GetMatch().(type) {
		case *AddressMatch_ParamExact:
			names = append(names, m.ParamExact)
		case *AddressMatch_ParamPrefix:
			names = append(names, m.ParamPrefix)
		}
	case *QueryFilter_Reference:
		addString(v.Reference.GetCond())
	case *QueryFilter_Ledger:
		addString(v.Ledger.GetCond())
	case *QueryFilter_BuiltinUint:
		addRange(v.BuiltinUint.GetCond().GetParamMin(), v.BuiltinUint.GetCond().GetParamMax())
	case *QueryFilter_LogBuiltinUint:
		addRange(v.LogBuiltinUint.GetCond().GetParamMin(), v.LogBuiltinUint.GetCond().GetParamMax())
	case *QueryFilter_LogId:
		addRange(v.LogId.GetCond().GetParamMin(), v.LogId.GetCond().GetParamMax())
	}

	return names
}
