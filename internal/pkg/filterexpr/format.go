package filterexpr

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

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
	case *commonpb.QueryFilter_AccountHasAsset:
		return formatAccountHasAsset(v.AccountHasAsset), precLeaf
	case *commonpb.QueryFilter_Ledger:
		return formatLedgerCondition(v.Ledger), precLeaf
	case *commonpb.QueryFilter_Audit:
		return formatAuditCondition(v.Audit)
	case *commonpb.QueryFilter_BuiltinUint:
		return formatBuiltinUintCondition(v.BuiltinUint)
	case *commonpb.QueryFilter_LogBuiltinUint:
		return formatLogBuiltinUintCondition(v.LogBuiltinUint)
	case *commonpb.QueryFilter_LogId:
		return formatUintRange("logId", v.LogId.GetCond(), renderRawUint)
	case *commonpb.QueryFilter_Reference:
		return "reference == " + formatStringCondValue(v.Reference.GetCond()), precLeaf
	case *commonpb.QueryFilter_Reverted:
		return fmt.Sprintf("reverted == %t", v.Reverted.GetValue()), precLeaf
	default:
		return "<unknown filter>", precLeaf
	}
}

// formatBuiltinUintCondition renders a transaction builtin uint range back into
// `field OP value`, the inverse of the parser's bare intrinsic fields: `id`
// bounds render as raw uints, the date fields (`timestamp`, `insertedAt`,
// `revertedAt`) as quoted RFC3339 (EN-1544).
func formatBuiltinUintCondition(bc *commonpb.BuiltinUintCondition) (string, int) {
	switch bc.GetField() {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:
		return formatUintRange("id", bc.GetCond(), renderRawUint)
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		return formatUintRange("timestamp", bc.GetCond(), renderDatetimeBound)
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		return formatUintRange("insertedAt", bc.GetCond(), renderDatetimeBound)
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
		return formatUintRange("revertedAt", bc.GetCond(), renderDatetimeBound)
	default:
		return "<unknown builtin field>", precLeaf
	}
}

// formatLogBuiltinUintCondition renders a log builtin uint range. The only field
// the textual grammar reads back is `date` (quoted RFC3339 output).
func formatLogBuiltinUintCondition(lc *commonpb.LogBuiltinUintCondition) (string, int) {
	switch lc.GetField() {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		return formatUintRange("date", lc.GetCond(), renderDatetimeBound)
	default:
		return "<unknown log field>", precLeaf
	}
}

// formatUintRange renders a bare-field uint range (transaction/log builtins and
// the uint/datetime audit fields) as `field OP value`, rendering each literal
// bound with render and each parameter bound as `$name` (non-audit fields only:
// the audit grammar rejects parameters).
//
// A closed range only renders as `between` when BOTH bounds are inclusive, since
// `between` parses back inclusive on both ends. If either bound is exclusive (the
// shape a JSON `$and` of `$gt`/`$lt` folds into) it renders as two comparison
// clauses joined by `and` — `field > lo and field < hi` — which the parser folds
// back into the same single condition (see foldClosedRangeAnd), so the
// exclusivity survives the round-trip instead of being silently widened. The
// field names are bare, disambiguated only by the re-parse target (EN-1549).
func formatUintRange(field string, uc *commonpb.UintCondition, render func(uint64) string) (string, int) {
	// A bound is a literal or a $param (ParamMin/ParamMax, resolved when a
	// prepared query executes). Rendering both kinds is what keeps a
	// parameterized range exportable: dropping the param side of a mixed range
	// would silently widen the re-applied filter.
	lo, hasLo := renderUintBound(uc.Min, uc.GetParamMin(), render)
	hi, hasHi := renderUintBound(uc.Max, uc.GetParamMax(), render)

	if hasLo && hasHi && lo == hi && !uc.GetMinExclusive() && !uc.GetMaxExclusive() {
		return fmt.Sprintf("%s == %s", field, lo), precLeaf
	}

	if hasLo && hasHi {
		if !uc.GetMinExclusive() && !uc.GetMaxExclusive() {
			return fmt.Sprintf("%s between %s and %s", field, lo, hi), precLeaf
		}

		// An exclusive bound folds into two comparison clauses joined by
		// `and`. Report precOr so every enclosing operator but `or`
		// parenthesizes the pair: a wrapping `not` would otherwise
		// mis-associate (`not a and b` reads as `(not a) and b`), and an
		// enclosing `and` would splice the two clauses into its own operand
		// list, where they no longer fold back into one condition. Under an
		// `or` the pair binds tighter anyway and still folds.
		return fmt.Sprintf("%s %s %s and %s %s %s",
			field, lowerOp(uc.GetMinExclusive()), lo,
			field, upperOp(uc.GetMaxExclusive()), hi), precOr
	}

	if hasLo {
		return fmt.Sprintf("%s %s %s", field, lowerOp(uc.GetMinExclusive()), lo), precLeaf
	}

	if hasHi {
		return fmt.Sprintf("%s %s %s", field, upperOp(uc.GetMaxExclusive()), hi), precLeaf
	}

	return field + " <uint?>", precLeaf
}

// renderUintBound renders one side of a uint range: `$name` for a parameter,
// render(literal) otherwise. ok=false when the side has no bound.
func renderUintBound(literal *uint64, param string, render func(uint64) string) (string, bool) {
	if param != "" {
		return "$" + param, true
	}
	if literal != nil {
		return render(*literal), true
	}

	return "", false
}

// renderDatetimeBound renders a microsecond bound as a quoted RFC3339 string
// when that form round-trips through the decoder, and falls back to the raw
// unsigned-microsecond form otherwise. The decoder (`commonpb.CoerceDatetimeMicros`)
// accepts the full uint64 raw range, but RFC3339 cannot represent every such
// value: `int64(v)` wraps for `v > math.MaxInt64` (yielding a pre-epoch time the
// decoder rejects), and years past 9999 format to a non-RFC3339 5-digit-year
// string that fails to parse back. In both cases the raw-uint form is the only
// rendering that survives Parse(Format(f)); we verify the round-trip against the
// actual decoder rather than guessing the boundary.
func renderDatetimeBound(v uint64) string {
	if v <= math.MaxInt64 {
		s := time.UnixMicro(int64(v)).UTC().Format(time.RFC3339Nano)
		if back, err := commonpb.CoerceDatetimeMicros(s); err == nil && back == v {
			return strconv.Quote(s)
		}
	}

	return strconv.FormatUint(v, 10)
}

// renderRawUint renders a plain-counter bound (ids, sequences) as a raw uint.
func renderRawUint(v uint64) string { return strconv.FormatUint(v, 10) }

// lowerOp / upperOp map a bound's exclusivity to its DSL comparison operator.
func lowerOp(exclusive bool) string {
	if exclusive {
		return ">"
	}

	return ">="
}

func upperOp(exclusive bool) string {
	if exclusive {
		return "<"
	}

	return "<="
}

// formatAuditCondition renders an AuditCondition back into the bare `field OP
// value` form, inverse of the parser's FieldCond production on the audit target
// (EN-1549). The `audit[...]` namespace prefix is gone: the audit fields are bare
// (`outcome == failure`, `ledger == main`, `timestamp >= "…"`). The output is
// only unambiguous when re-parsed on the audit target — `ledger`/`timestamp`
// collide with the transaction/log arms otherwise — which is exactly the contract
// (an audit filter is always re-parsed with QUERY_TARGET_AUDIT).
func formatAuditCondition(ac *commonpb.AuditCondition) (string, int) {
	key, ok := commonpb.AuditFieldName(ac.GetField())
	if !ok {
		return "<unknown audit field>", precLeaf
	}

	switch cond := ac.GetCondition().(type) {
	case *commonpb.AuditCondition_StringCond:
		return fmt.Sprintf("%s == %s", key, formatStringCondValue(cond.StringCond)), precLeaf
	case *commonpb.AuditCondition_UintCond:
		// The timestamp field is a datetime: its bounds render as quoted RFC3339
		// so the output round-trips through the datetime-aware parser.
		render := renderRawUint
		if ac.GetField() == commonpb.AuditField_AUDIT_FIELD_TIMESTAMP {
			render = renderDatetimeBound
		}

		return formatUintRange(key, cond.UintCond, render)
	case *commonpb.AuditCondition_StringPrefix:
		return fmt.Sprintf("%s ^= %s", key, quoteIfNeeded(cond.StringPrefix)), precLeaf
	default:
		return key + " <unknown>", precLeaf
	}
}

// formatLedgerCondition renders a non-audit LedgerCondition as `ledger == value`,
// the inverse of the parser's `ledger == VALUE` production (a bare `ledger` field
// on a non-audit target). The value renders as a `$param` reference or a
// quote-if-needed hardcoded string, the same value path every other string
// condition uses. On the audit target the ledger field is carried by the
// AuditCondition arm instead (formatAuditCondition), so this only ever sees the
// transaction/log/account ledger condition.
func formatLedgerCondition(lc *commonpb.LedgerCondition) string {
	return "ledger == " + formatStringCondValue(lc.GetCond())
}

// formatAccountHasAsset renders an AccountHasAssetCondition as `has asset BASE`
// (precision 0) or `has asset BASE/PRECISION`. Inverse of the parser's
// `has asset <asset>` production.
func formatAccountHasAsset(c *commonpb.AccountHasAssetCondition) string {
	if c.GetPrecision() == 0 {
		return "has asset " + c.GetAssetBase()
	}

	return fmt.Sprintf("has asset %s/%d", c.GetAssetBase(), c.GetPrecision())
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
	key := quoteIfNeeded(fc.GetField().GetMetadata())
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
	key := quoteIfNeeded(fc.GetField().GetMetadata())

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

// quoteIfNeeded wraps a value in double quotes unless it can be emitted as a bare
// token that Parse reads back as the same value. Since a bare Ident is
// plain-alphanumeric (EN-1547), anything with a special char (`-`, `:`, `.`, `/`,
// spaces, …) must be quoted to round-trip.
func quoteIfNeeded(s string) string {
	if needsQuoting(s) {
		return `"` + s + `"`
	}

	return s
}

var (
	// bareIdent matches exactly the lexer's plain-alphanumeric Ident. A value is
	// safe to emit unquoted only if it is a whole bare Ident (and not a structural
	// operator, below); anything with a special char, a leading digit, or empty
	// must be quoted or it would fail to reparse. Kept in lockstep with the Ident
	// rule in parser.go.
	bareIdent = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

	// reservedOperators is the set of structural keywords that the value grammar
	// does NOT accept as a bare value (Value.Kw admits the noun words and
	// Value.Bool the booleans, but the operators must keep terminating
	// expressions). A value equal to one of these must be quoted. Kept in lockstep
	// with the lexer Keyword rule minus the noun/boolean words.
	reservedOperators = map[string]bool{
		"and": true, "or": true, "not": true, "in": true, "between": true,
	}
)

// needsQuoting reports whether a value must be double-quoted to survive a
// Format→Parse round-trip: it quotes anything that is not a clean bare Ident, and
// also quotes a bare Ident that collides with a structural operator. Conservative
// by design — over-quoting is harmless, under-quoting is a round-trip bug.
func needsQuoting(s string) bool {
	return !bareIdent.MatchString(s) || reservedOperators[s]
}
