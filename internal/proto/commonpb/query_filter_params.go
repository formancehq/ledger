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
// references, across every condition kind that can carry one. Combinators
// return nothing; callers walk the tree.
func FilterLeafParamNames(f *QueryFilter) []string {
	var names []string

	add := func(name string) {
		if name != "" {
			names = append(names, name)
		}
	}
	addUint := func(uc *UintCondition) {
		add(uc.GetParamMin())
		add(uc.GetParamMax())
	}

	switch v := f.GetFilter().(type) {
	case *QueryFilter_Field:
		switch c := v.Field.GetCondition().(type) {
		case *FieldCondition_StringCond:
			add(c.StringCond.GetParam())
		case *FieldCondition_BoolCond:
			add(c.BoolCond.GetParam())
		case *FieldCondition_IntCond:
			add(c.IntCond.GetParamMin())
			add(c.IntCond.GetParamMax())
		case *FieldCondition_UintCond:
			addUint(c.UintCond)
		}
	case *QueryFilter_Address:
		add(v.Address.GetParamExact())
		add(v.Address.GetParamPrefix())
	case *QueryFilter_Reference:
		add(v.Reference.GetCond().GetParam())
	case *QueryFilter_Ledger:
		add(v.Ledger.GetCond().GetParam())
	case *QueryFilter_BuiltinUint:
		addUint(v.BuiltinUint.GetCond())
	case *QueryFilter_LogBuiltinUint:
		addUint(v.LogBuiltinUint.GetCond())
	case *QueryFilter_LogId:
		addUint(v.LogId.GetCond())
	}

	return names
}
