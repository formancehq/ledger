package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Prepared queries are named, stored filter templates: a create/update/delete
// mutates a ledger-scoped registry through the ordinary bulk path, and
// ExecutePreparedQuery runs the stored filter — with the caller's parameter
// values bound into its param leaves — through the same compiler the ad-hoc
// list queries use.
//
// The model side is deliberately thin. Rather than teach the evaluator a second
// grammar, the generator builds a CONCRETE filter with the ad-hoc generators and
// then parameterizes it: a random subset of hardcoded leaves becomes param
// references, and the displaced values become the parameter map. Substitution is
// the exact inverse, so validation reuses matchAccountFilter / matchTxFilter /
// matchLogFilter untouched — see substituteParams.
//
// Two properties make the validation honest under concurrency:
//
//   - The stored filter is read from the CANDIDATE BASE, never from a
//     driver-side copy. A concurrent update makes the stored filter
//     base-dependent, and the execution window follows the filter.
//   - A base whose filter needs a parameter the call did not supply predicts a
//     compilation error, not a window. That falls out of substituteParams
//     reporting failure, so the missing-parameter path needs no special casing.

// preparedQueryNamePool is small on purpose: with a handful of names shared
// across the fleet, create/update/delete collide constantly, so the
// ALREADY_EXISTS and NOT_FOUND rejections are exercised by ordinary churn
// rather than by a dedicated probe.
var preparedQueryNamePool = []uint8{0, 1, 2, 3, 4}

func preparedQueryName() string {
	return fmt.Sprintf("pq%d", random.RandomChoice(preparedQueryNamePool))
}

// preparedQueryTargets are the targets the FSM lets a prepared query be stored
// on (domain.IsPreparedQueryExecutableTarget). AUDIT is rejected at write time.
var preparedQueryTargets = []commonpb.QueryTarget{
	commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
	commonpb.QueryTarget_QUERY_TARGET_LOGS,
}

func createPreparedQueryReq(ledger string, q *commonpb.PreparedQuery) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{
		CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{Ledger: ledger, Query: q},
	}}
}

func updatePreparedQueryReq(ledger, name string, filter *commonpb.QueryFilter) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{
		UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: ledger, Name: name, Filter: filter},
	}}
}

func deletePreparedQueryReq(ledger, name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{
		DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: ledger, Name: name},
	}}
}

// rollPreparedQueryOp: ~1-in-16 a bulk is a prepared-query registry op rather
// than ledger traffic, churning the registry the execution reads probe.
func rollPreparedQueryOp() bool {
	return oneIn(16)
}

// generatePreparedQueryOp picks a name from the pool and moves it through the
// lifecycle: create when the ledger lacks it, else update (rewriting the filter,
// which is what an execution must follow) or occasionally delete. Reads
// committed state only, so it may run without the checker lock.
//
// The rolls deliberately also produce the rejected cases the model predicts:
// a create on a name already present (ALREADY_EXISTS) and an update or delete
// on one that is absent (NOT_FOUND).
func generatePreparedQueryOp(g oracle.GlobalState, ledger string) *servicepb.Request {
	ls := g.Ledger(ledger)
	name := preparedQueryName()

	stored, exists := ls.PreparedQuery(name)
	if !exists {
		target := random.RandomChoice(preparedQueryTargets)

		filter := genPreparedQueryFilter(ls, ledger, target)
		if filter == nil {
			return nil
		}

		return createPreparedQueryReq(ledger, &commonpb.PreparedQuery{
			Name:   name,
			Target: target,
			Filter: filter,
		})
	}

	if oneIn(4) {
		return deletePreparedQueryReq(ledger, name)
	}

	// An update carries no target: the new filter must be valid on the STORED
	// one, which is what the FSM validates it against.
	filter := genPreparedQueryFilter(ls, ledger, stored.GetTarget())
	if filter == nil {
		return nil
	}

	return updatePreparedQueryReq(ledger, name, filter)
}

// genPreparedQueryFilter builds a storable filter for target: a concrete filter
// from the ad-hoc generators, with a random subset of its leaves rewritten into
// parameter references.
//
// Filters carrying a condition invalid on the target are discarded rather than
// stored — the FSM rejects those at write time (ValidateFilterForTarget), so
// storing one would only ever exercise the write-side rejection the ad-hoc
// generators already cover. Returns nil when the roll produced nothing storable;
// the caller then emits no op this round.
func genPreparedQueryFilter(ls oracle.LedgerState, ledger string, target commonpb.QueryTarget) *commonpb.QueryFilter {
	var concrete *commonpb.QueryFilter

	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		concrete = genAccountFilter(sampleFieldSeeds(ls, target))
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		concrete = genTransactionFilter(txFilterSeedsOf(ls))
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		concrete = genLogFilter(ledger, 0)
	default:
		panic(fmt.Sprintf("genPreparedQueryFilter: non-executable target %v", target))
	}

	// A nil filter is storable (it means "no filter") but only on create — an
	// update rejects it. Skip the round rather than split the two paths.
	if concrete == nil || filterInvalidForTarget(concrete, target) || !bareOrNoHasAsset(concrete) {
		return nil
	}

	// The displaced values are deliberately thrown away: only the filter is
	// stored, and every execution binds fresh values discovered from the stored
	// filter itself (genPreparedParams). Keeping a driver-side copy would be a
	// second source of truth that a concurrent update immediately invalidates.
	return parameterizeFilter(concrete, preparedParams{})
}

// --- parameterization ----------------------------------------------------

// preparedParams is the parameter map an ExecutePreparedQuery call carries.
type preparedParams map[string]*commonpb.ParameterValue

// paramKind names the value shape a parameter reference expects. Address is
// split out from the generic string kind only so generated values land in the
// account-address space, where they match something.
type paramKind int

const (
	paramKindAddress paramKind = iota
	paramKindString
	paramKindUint
	paramKindInt
	paramKindBool
)

// paramNeed is one parameter reference a stored filter carries.
type paramNeed struct {
	name string
	kind paramKind
}

// paramizer hands out parameter names and collects the values they displace.
type paramizer struct {
	params preparedParams
	n      int
}

func (pz *paramizer) next() string {
	name := fmt.Sprintf("p%d", pz.n)
	pz.n++

	return name
}

func (pz *paramizer) put(name string, v *commonpb.ParameterValue) string {
	pz.params[name] = v

	return name
}

// paramOdds: each eligible leaf value is parameterized with probability 1/2, so
// a filter tree yields a mix of hardcoded and parameterized leaves.
func rollParameterize() bool { return oneIn(2) }

// parameterizeFilter returns a copy of f with a random subset of its hardcoded
// leaf values rewritten into parameter references, recording each displaced
// value in params. It is the inverse of substituteParams: for the params it
// fills, substituteParams(parameterizeFilter(f, params), params) reproduces f.
func parameterizeFilter(f *commonpb.QueryFilter, params preparedParams) *commonpb.QueryFilter {
	if f == nil {
		return nil
	}

	out := f.CloneVT()
	pz := &paramizer{params: params}
	parameterizeInto(out, pz)

	return out
}

func parameterizeInto(f *commonpb.QueryFilter, pz *paramizer) {
	switch c := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range c.And.GetFilters() {
			parameterizeInto(child, pz)
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range c.Or.GetFilters() {
			parameterizeInto(child, pz)
		}
	case *commonpb.QueryFilter_Not:
		parameterizeInto(c.Not.GetFilter(), pz)
	case *commonpb.QueryFilter_Address:
		parameterizeAddress(c.Address, pz)
	case *commonpb.QueryFilter_Reference:
		parameterizeString(c.Reference.GetCond(), pz)
	case *commonpb.QueryFilter_Ledger:
		parameterizeString(c.Ledger.GetCond(), pz)
	case *commonpb.QueryFilter_LogId:
		parameterizeUint(c.LogId.GetCond(), pz)
	case *commonpb.QueryFilter_BuiltinUint:
		parameterizeUint(c.BuiltinUint.GetCond(), pz)
	case *commonpb.QueryFilter_LogBuiltinUint:
		parameterizeUint(c.LogBuiltinUint.GetCond(), pz)
	case *commonpb.QueryFilter_Field:
		parameterizeField(c.Field, pz)
	}
	// AccountHasAsset, Reverted and Audit carry no parameterizable value.
}

func parameterizeAddress(am *commonpb.AddressMatch, pz *paramizer) {
	if am == nil || !rollParameterize() {
		return
	}

	switch m := am.GetMatch().(type) {
	case *commonpb.AddressMatch_HardcodedPrefix:
		am.Match = &commonpb.AddressMatch_ParamPrefix{
			ParamPrefix: pz.put(pz.next(), stringParam(m.HardcodedPrefix)),
		}
	case *commonpb.AddressMatch_HardcodedExact:
		am.Match = &commonpb.AddressMatch_ParamExact{
			ParamExact: pz.put(pz.next(), stringParam(m.HardcodedExact)),
		}
	}
}

func parameterizeString(sc *commonpb.StringCondition, pz *paramizer) {
	if sc == nil || !rollParameterize() {
		return
	}

	if m, ok := sc.GetValue().(*commonpb.StringCondition_Hardcoded); ok {
		sc.Value = &commonpb.StringCondition_Param{Param: pz.put(pz.next(), stringParam(m.Hardcoded))}
	}
}

func parameterizeBool(bc *commonpb.BoolCondition, pz *paramizer) {
	if bc == nil || !rollParameterize() {
		return
	}

	if m, ok := bc.GetValue().(*commonpb.BoolCondition_Hardcoded); ok {
		bc.Value = &commonpb.BoolCondition_Param{Param: pz.put(pz.next(), boolParam(m.Hardcoded))}
	}
}

// parameterizeUint moves each present bound onto a parameter independently, so
// a two-sided range can end up half hardcoded. The bound itself is cleared: the
// compiler reads the parameter when the param name is set, and leaving the
// literal behind would make the inverse ambiguous.
func parameterizeUint(uc *commonpb.UintCondition, pz *paramizer) {
	if uc == nil {
		return
	}

	if uc.Min != nil && rollParameterize() {
		uc.ParamMin = pz.put(pz.next(), uintParam(uc.GetMin()))
		uc.Min = nil
	}

	if uc.Max != nil && rollParameterize() {
		uc.ParamMax = pz.put(pz.next(), uintParam(uc.GetMax()))
		uc.Max = nil
	}
}

func parameterizeInt(ic *commonpb.IntCondition, pz *paramizer) {
	if ic == nil {
		return
	}

	if ic.Min != nil && rollParameterize() {
		ic.ParamMin = pz.put(pz.next(), intParam(ic.GetMin()))
		ic.Min = nil
	}

	if ic.Max != nil && rollParameterize() {
		ic.ParamMax = pz.put(pz.next(), intParam(ic.GetMax()))
		ic.Max = nil
	}
}

func parameterizeField(fc *commonpb.FieldCondition, pz *paramizer) {
	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		parameterizeString(c.StringCond, pz)
	case *commonpb.FieldCondition_IntCond:
		parameterizeInt(c.IntCond, pz)
	case *commonpb.FieldCondition_UintCond:
		parameterizeUint(c.UintCond, pz)
	case *commonpb.FieldCondition_BoolCond:
		parameterizeBool(c.BoolCond, pz)
	}
	// ExistsCond carries no value.
}

func stringParam(v string) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: v}}
}

func uintParam(v uint64) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: v}}
}

func intParam(v int64) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Int64Value{Int64Value: v}}
}

func boolParam(v bool) *commonpb.ParameterValue {
	return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_BoolValue{BoolValue: v}}
}

// --- substitution (the inverse) ------------------------------------------

// substituteParams returns a copy of f with every parameter reference replaced
// by its bound value, and reports whether every reference resolved. It mirrors
// the compiler's resolution rules: a reference to a parameter that was not
// supplied, or one supplied with the wrong value type, is a compilation error
// (extractString / extractUint64 / extractInt64 in internal/query/compile.go) —
// here that surfaces as ok=false, so a base whose stored filter needs a missing
// parameter predicts a rejection rather than a window.
func substituteParams(f *commonpb.QueryFilter, params preparedParams) (*commonpb.QueryFilter, bool) {
	if f == nil {
		return nil, true
	}

	out := f.CloneVT()
	ok := substituteInto(out, params)

	return out, ok
}

func substituteInto(f *commonpb.QueryFilter, params preparedParams) bool {
	switch c := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		return substituteChildren(c.And.GetFilters(), params)
	case *commonpb.QueryFilter_Or:
		return substituteChildren(c.Or.GetFilters(), params)
	case *commonpb.QueryFilter_Not:
		return substituteInto(c.Not.GetFilter(), params)
	case *commonpb.QueryFilter_Address:
		return substituteAddress(c.Address, params)
	case *commonpb.QueryFilter_Reference:
		return substituteString(c.Reference.GetCond(), params)
	case *commonpb.QueryFilter_Ledger:
		return substituteString(c.Ledger.GetCond(), params)
	case *commonpb.QueryFilter_LogId:
		return substituteUint(c.LogId.GetCond(), params)
	case *commonpb.QueryFilter_BuiltinUint:
		return substituteUint(c.BuiltinUint.GetCond(), params)
	case *commonpb.QueryFilter_LogBuiltinUint:
		return substituteUint(c.LogBuiltinUint.GetCond(), params)
	case *commonpb.QueryFilter_Field:
		return substituteField(c.Field, params)
	default:
		return true
	}
}

// substituteChildren resolves every child rather than stopping at the first
// failure: the compiler walks the whole tree, and an unresolved reference
// anywhere rejects the query regardless of where it sits.
func substituteChildren(children []*commonpb.QueryFilter, params preparedParams) bool {
	ok := true
	for _, child := range children {
		if !substituteInto(child, params) {
			ok = false
		}
	}

	return ok
}

func substituteAddress(am *commonpb.AddressMatch, params preparedParams) bool {
	switch m := am.GetMatch().(type) {
	case *commonpb.AddressMatch_ParamPrefix:
		v, ok := lookupString(params, m.ParamPrefix)
		if !ok {
			return false
		}

		am.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: v}
	case *commonpb.AddressMatch_ParamExact:
		v, ok := lookupString(params, m.ParamExact)
		if !ok {
			return false
		}

		am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: v}
	}

	return true
}

func substituteString(sc *commonpb.StringCondition, params preparedParams) bool {
	m, isParam := sc.GetValue().(*commonpb.StringCondition_Param)
	if !isParam {
		return true
	}

	v, ok := lookupString(params, m.Param)
	if !ok {
		return false
	}

	sc.Value = &commonpb.StringCondition_Hardcoded{Hardcoded: v}

	return true
}

func substituteBool(bc *commonpb.BoolCondition, params preparedParams) bool {
	m, isParam := bc.GetValue().(*commonpb.BoolCondition_Param)
	if !isParam {
		return true
	}

	pv, present := params[m.Param]
	if !present {
		return false
	}

	v, isBool := pv.GetValue().(*commonpb.ParameterValue_BoolValue)
	if !isBool {
		return false
	}

	bc.Value = &commonpb.BoolCondition_Hardcoded{Hardcoded: v.BoolValue}

	return true
}

func substituteUint(uc *commonpb.UintCondition, params preparedParams) bool {
	if uc == nil {
		return true
	}

	ok := true

	if uc.GetParamMin() != "" {
		if v, found := lookupUint(params, uc.GetParamMin()); found {
			uc.Min = &v
			uc.ParamMin = ""
		} else {
			ok = false
		}
	}

	if uc.GetParamMax() != "" {
		if v, found := lookupUint(params, uc.GetParamMax()); found {
			uc.Max = &v
			uc.ParamMax = ""
		} else {
			ok = false
		}
	}

	return ok
}

func substituteInt(ic *commonpb.IntCondition, params preparedParams) bool {
	if ic == nil {
		return true
	}

	ok := true

	if ic.GetParamMin() != "" {
		if v, found := lookupInt(params, ic.GetParamMin()); found {
			ic.Min = &v
			ic.ParamMin = ""
		} else {
			ok = false
		}
	}

	if ic.GetParamMax() != "" {
		if v, found := lookupInt(params, ic.GetParamMax()); found {
			ic.Max = &v
			ic.ParamMax = ""
		} else {
			ok = false
		}
	}

	return ok
}

func substituteField(fc *commonpb.FieldCondition, params preparedParams) bool {
	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		return substituteString(c.StringCond, params)
	case *commonpb.FieldCondition_IntCond:
		return substituteInt(c.IntCond, params)
	case *commonpb.FieldCondition_UintCond:
		return substituteUint(c.UintCond, params)
	case *commonpb.FieldCondition_BoolCond:
		return substituteBool(c.BoolCond, params)
	default:
		return true
	}
}

func lookupString(params preparedParams, name string) (string, bool) {
	pv, present := params[name]
	if !present {
		return "", false
	}

	v, isString := pv.GetValue().(*commonpb.ParameterValue_StringValue)
	if !isString {
		return "", false
	}

	return v.StringValue, true
}

func lookupUint(params preparedParams, name string) (uint64, bool) {
	pv, present := params[name]
	if !present {
		return 0, false
	}

	v, isUint := pv.GetValue().(*commonpb.ParameterValue_Uint64Value)
	if !isUint {
		return 0, false
	}

	return v.Uint64Value, true
}

func lookupInt(params preparedParams, name string) (int64, bool) {
	pv, present := params[name]
	if !present {
		return 0, false
	}

	v, isInt := pv.GetValue().(*commonpb.ParameterValue_Int64Value)
	if !isInt {
		return 0, false
	}

	return v.Int64Value, true
}

// --- parameter discovery -------------------------------------------------

// collectParams walks a stored filter and returns every parameter reference it
// carries, in tree order. The execution path generates a value per need; the
// stored filter is the only source of truth for what a query expects, since the
// driver keeps no copy of what it created.
func collectParams(f *commonpb.QueryFilter) []paramNeed {
	var out []paramNeed
	collectParamsInto(f, &out)

	return out
}

func collectParamsInto(f *commonpb.QueryFilter, out *[]paramNeed) {
	switch c := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range c.And.GetFilters() {
			collectParamsInto(child, out)
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range c.Or.GetFilters() {
			collectParamsInto(child, out)
		}
	case *commonpb.QueryFilter_Not:
		collectParamsInto(c.Not.GetFilter(), out)
	case *commonpb.QueryFilter_Address:
		switch m := c.Address.GetMatch().(type) {
		case *commonpb.AddressMatch_ParamPrefix:
			*out = append(*out, paramNeed{m.ParamPrefix, paramKindAddress})
		case *commonpb.AddressMatch_ParamExact:
			*out = append(*out, paramNeed{m.ParamExact, paramKindAddress})
		}
	case *commonpb.QueryFilter_Reference:
		collectStringParam(c.Reference.GetCond(), out)
	case *commonpb.QueryFilter_Ledger:
		collectStringParam(c.Ledger.GetCond(), out)
	case *commonpb.QueryFilter_LogId:
		collectUintParams(c.LogId.GetCond(), out)
	case *commonpb.QueryFilter_BuiltinUint:
		collectUintParams(c.BuiltinUint.GetCond(), out)
	case *commonpb.QueryFilter_LogBuiltinUint:
		collectUintParams(c.LogBuiltinUint.GetCond(), out)
	case *commonpb.QueryFilter_Field:
		collectFieldParams(c.Field, out)
	}
}

func collectStringParam(sc *commonpb.StringCondition, out *[]paramNeed) {
	if m, ok := sc.GetValue().(*commonpb.StringCondition_Param); ok {
		*out = append(*out, paramNeed{m.Param, paramKindString})
	}
}

func collectUintParams(uc *commonpb.UintCondition, out *[]paramNeed) {
	if uc.GetParamMin() != "" {
		*out = append(*out, paramNeed{uc.GetParamMin(), paramKindUint})
	}

	if uc.GetParamMax() != "" {
		*out = append(*out, paramNeed{uc.GetParamMax(), paramKindUint})
	}
}

func collectIntParams(ic *commonpb.IntCondition, out *[]paramNeed) {
	if ic.GetParamMin() != "" {
		*out = append(*out, paramNeed{ic.GetParamMin(), paramKindInt})
	}

	if ic.GetParamMax() != "" {
		*out = append(*out, paramNeed{ic.GetParamMax(), paramKindInt})
	}
}

func collectFieldParams(fc *commonpb.FieldCondition, out *[]paramNeed) {
	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		collectStringParam(c.StringCond, out)
	case *commonpb.FieldCondition_IntCond:
		collectIntParams(c.IntCond, out)
	case *commonpb.FieldCondition_UintCond:
		collectUintParams(c.UintCond, out)
	case *commonpb.FieldCondition_BoolCond:
		if m, ok := c.BoolCond.GetValue().(*commonpb.BoolCondition_Param); ok {
			*out = append(*out, paramNeed{m.Param, paramKindBool})
		}
	}
}

// --- diagnostics ---------------------------------------------------------

// describePreparedQueries renders a registry listing for a finding's details,
// name-sorted so the model and server renderings line up side by side.
func describePreparedQueries(queries []*commonpb.PreparedQuery) string {
	sorted := make([]*commonpb.PreparedQuery, len(queries))
	copy(sorted, queries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].GetName() < sorted[j].GetName() })

	parts := make([]string, 0, len(sorted))
	for _, q := range sorted {
		parts = append(parts, fmt.Sprintf("%s[%s]=%s",
			q.GetName(), describeQueryTarget(q.GetTarget()), describeFilter(q.GetFilter())))
	}

	return strings.Join(parts, " ")
}

func describeQueryTarget(t commonpb.QueryTarget) string {
	switch t {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return "accounts"
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return "transactions"
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		return "logs"
	default:
		return t.String()
	}
}

// describeParams renders a parameter map for a finding's details, name-sorted.
func describeParams(params preparedParams) string {
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}

	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+describeParamValue(params[name]))
	}

	return strings.Join(parts, ",")
}

func describeParamValue(v *commonpb.ParameterValue) string {
	switch t := v.GetValue().(type) {
	case *commonpb.ParameterValue_StringValue:
		return "s:" + t.StringValue
	case *commonpb.ParameterValue_Uint64Value:
		return fmt.Sprintf("u:%d", t.Uint64Value)
	case *commonpb.ParameterValue_Int64Value:
		return fmt.Sprintf("i:%d", t.Int64Value)
	case *commonpb.ParameterValue_BoolValue:
		return fmt.Sprintf("b:%t", t.BoolValue)
	default:
		return "?"
	}
}

// --- execution-time parameter binding ------------------------------------

// genPreparedParams binds a value for every parameter the stored filter
// references, drawing from the committed state so bound windows are populated
// rather than trivially empty. Returns the map and whether the binding is
// complete: ~1-in-8 a need is deliberately left unbound or bound with the wrong
// value type, which the compiler must reject — the negative path the validators
// predict through substituteParams reporting failure.
//
// The stored filter is the only source of truth for what a query expects: the
// driver keeps no record of what it created, and a concurrent update may have
// rewritten the references since.
func genPreparedParams(ls oracle.LedgerState, stored *commonpb.QueryFilter) (preparedParams, bool) {
	needs := collectParams(stored)
	params := make(preparedParams, len(needs))

	if len(needs) == 0 {
		return params, true
	}

	// At most one need is spoiled per call, so the rejection is attributable.
	spoil := -1
	if oneIn(8) {
		spoil = internal.Rand().Intn(len(needs))
	}

	for i, need := range needs {
		if i == spoil {
			if oneIn(2) {
				// Omitted entirely: "parameter %q not provided".
				continue
			}

			// Bound with a type the condition cannot consume.
			params[need.name] = mistypedParam(need.kind)

			continue
		}

		params[need.name] = genParamValue(ls, need.kind)
	}

	return params, spoil < 0
}

// genParamValue draws a plausible value for one parameter kind. Address values
// come from the ledger's own account universe where it has one, so an address
// parameter usually selects something instead of an empty window.
func genParamValue(ls oracle.LedgerState, kind paramKind) *commonpb.ParameterValue {
	switch kind {
	case paramKindAddress:
		return stringParam(sampleCommittedAddress(ls))
	case paramKindString:
		return stringParam(sampleStringValue(ls))
	case paramKindUint:
		return uintParam(sampleUint(ls))
	case paramKindInt:
		return intParam(int64(random.RandomChoice([]uint8{0, 1, 2, 5, 10})) - 4)
	default:
		return boolParam(oneIn(2))
	}
}

// mistypedParam returns a value of a type the kind's condition rejects, so the
// compiler's type gate is exercised as well as its presence gate.
func mistypedParam(kind paramKind) *commonpb.ParameterValue {
	if kind == paramKindUint || kind == paramKindInt {
		return stringParam("not-a-number")
	}

	return uintParam(1)
}

// sampleCommittedAddress picks a committed account address, falling back to the
// address pool on an empty ledger. Distinct from actions.go's sampleAddress,
// which renders a fresh address from a chart pattern.
func sampleCommittedAddress(ls oracle.LedgerState) string {
	universe := accountUniverse(ls)
	if len(universe) == 0 {
		return poolAddress()
	}

	return universe[internal.Rand().Intn(len(universe))]
}

// sampleStringValue picks a value a string condition may match: a committed
// transaction reference where one exists, else a pool address.
func sampleStringValue(ls oracle.LedgerState) string {
	seeds := txFilterSeedsOf(ls)
	if len(seeds.refs) == 0 {
		return sampleCommittedAddress(ls)
	}

	return seeds.refs[internal.Rand().Intn(len(seeds.refs))]
}

// sampleUint picks a bound a uint condition may straddle: a known date stamp,
// else a small id-space number.
func sampleUint(ls oracle.LedgerState) uint64 {
	seeds := txFilterSeedsOf(ls)
	if len(seeds.stamps) == 0 {
		return uint64(random.RandomChoice([]uint8{0, 1, 2, 3, 5, 10}))
	}

	return seeds.stamps[internal.Rand().Intn(len(seeds.stamps))]
}

// bareOrNoHasAsset reports whether f is storable under the has-asset rule the
// validators rely on: the leaf may appear only as the whole filter, never
// inside a boolean. The has-asset index serves accounts that may have been
// purged from the volume table, while every other accounts leaf scans the live
// universe, so a boolean of the two is the read-store's own iterator
// intersection — set semantics the model would have to reimplement the
// read-store to predict. genAccountAssetFilter already emits only bare leaves;
// this holds the invariant at the storage boundary instead of assuming it.
func bareOrNoHasAsset(f *commonpb.QueryFilter) bool {
	if _, _, bare := hasAssetTarget(f); bare {
		return true
	}

	return !anyLeaf(f, func(leaf *commonpb.QueryFilter) bool {
		_, isHasAsset := leaf.GetFilter().(*commonpb.QueryFilter_AccountHasAsset)

		return isHasAsset
	})
}
