package commonpb

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

// QueryFilter has a hand-written JSON codec (this file) that mirrors the shared
// Formance query DSL (go-libs/pkg/query, as used by ledger v2) instead of
// leaking the protobuf-internal oneof/wrapper names through protojson. The
// canonical public shape is:
//
//	combinators:
//	  {"$and": [QueryFilter, ...]}
//	  {"$or":  [QueryFilter, ...]}
//	  {"$not": QueryFilter}
//	leaf conditions (single-key operator -> {field: value}):
//	  {"$match":  {"<field>": <value>}}   exact / prefix / bool match
//	  {"$gt"/"$gte"/"$lt"/"$lte": {"<field>": <value>}}   range bounds
//	  {"$exists": {"metadata": "<key>"}}  metadata key presence
//	  {"$exists": {"asset": "<assetRef>"}} account has-asset presence
//	  {"$in":     {"<field>": [<value>, ...]}}  (reserved; not yet emitted)
//	  {"$like":   {"<field>": <value>}}         (reserved; not yet emitted)
//
// Fields:
//   - metadata: `metadata[<key>]`
//   - address:  `address` (trailing ":" = prefix, else exact); `source` /
//     `destination` for role-scoped address matches
//   - `reference`, `reverted` (bool), `ledger`
//   - built-in transaction uint fields: `id`, `timestamp`, `insertedAt`,
//     `revertedAt`; log fields: `logId`, `date`
//   - `asset` (has-asset, via $exists)
//
// A value is either a JSON literal or a parameter reference `{"$param": "name"}`
// (prepared-query parameters resolved at execution time).
//
// A closed inclusive/exclusive range is expressed the v2 way — as an $and of
// the two bounds, e.g. `{"$and": [{"$gte": {"timestamp": 1}}, {"$lte":
// {"timestamp": 9}}]}` — and the decoder folds a same-field $gt/$gte + $lt/$lte
// pair back into a single range proto condition.

const (
	opAnd    = "$and"
	opOr     = "$or"
	opNot    = "$not"
	opMatch  = "$match"
	opGt     = "$gt"
	opGte    = "$gte"
	opLt     = "$lt"
	opLte    = "$lte"
	opExists = "$exists"
	opIn     = "$in"
	opLike   = "$like"
	opParam  = "$param"
)

// built-in transaction uint field names <-> proto enum. Only the fields the
// compiler resolves as an unsigned range are exposed (see BuiltinUintCondition
// in common.proto); the address/reference index kinds are matched via their own
// conditions.
var (
	txBuiltinFieldToJSON = map[TransactionBuiltinIndex]string{
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:          "id",
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:   "timestamp",
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT: "insertedAt",
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT: "revertedAt",
	}
	txBuiltinFieldFromJSON = invertEnumMap(txBuiltinFieldToJSON)
)

func invertEnumMap[E comparable](m map[E]string) map[string]E {
	out := make(map[string]E, len(m))
	for k, v := range m {
		out[v] = k
	}

	return out
}

// metadataKey builds the `metadata[<key>]` field name.
func metadataKey(key string) string { return "metadata[" + key + "]" }

// parseMetadataKey returns the inner key of a `metadata[<key>]` field name.
func parseMetadataKey(field string) (string, bool) {
	if len(field) > len("metadata[]") && field[:len("metadata[")] == "metadata[" && field[len(field)-1] == ']' {
		return field[len("metadata[") : len(field)-1], true
	}

	return "", false
}

// jsonValue encodes a condition value: either a literal or a {"$param":"name"}
// reference. On decode, exactly one of the two is populated.
type jsonValue struct {
	literal json.RawMessage
	param   string
}

func literalValue(v any) (jsonValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return jsonValue{}, err
	}

	return jsonValue{literal: b}, nil
}

func paramValue(name string) jsonValue { return jsonValue{param: name} }

func (v jsonValue) MarshalJSON() ([]byte, error) {
	if v.param != "" {
		return json.Marshal(map[string]string{opParam: v.param})
	}
	if len(v.literal) == 0 {
		return nil, errors.New("value: empty")
	}

	return v.literal, nil
}

func (v *jsonValue) UnmarshalJSON(data []byte) error {
	// A {"$param": "name"} object is a parameter reference; anything else is a
	// literal. We only treat an object with exactly the $param key as a param —
	// a plain object literal (unused today) would pass through as a literal.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err == nil {
		if raw, ok := probe[opParam]; ok {
			if len(probe) != 1 {
				return errors.New("$param: must be the only key")
			}
			var name string
			if err := json.Unmarshal(raw, &name); err != nil {
				return fmt.Errorf("$param: %w", err)
			}
			if name == "" {
				return errors.New("$param: name is required")
			}
			v.param = name

			return nil
		}
	}

	v.literal = append([]byte(nil), data...)

	return nil
}

func (v jsonValue) isParam() bool { return v.param != "" }

func (v jsonValue) asString() (string, error) {
	var s string
	if err := json.Unmarshal(v.literal, &s); err != nil {
		return "", fmt.Errorf("expected string value: %w", err)
	}

	return s, nil
}

func (v jsonValue) asBool() (bool, error) {
	var b bool
	if err := json.Unmarshal(v.literal, &b); err != nil {
		return false, fmt.Errorf("expected bool value: %w", err)
	}

	return b, nil
}

func (v jsonValue) asInt64() (int64, error) {
	var n int64
	if err := json.Unmarshal(v.literal, &n); err != nil {
		return 0, fmt.Errorf("expected integer value: %w", err)
	}

	return n, nil
}

// asUint64 accepts a JSON number or a decimal string (used for uint64 bounds to
// stay lossless above 2^53).
func (v jsonValue) asUint64() (uint64, error) {
	var s string
	if err := json.Unmarshal(v.literal, &s); err == nil {
		n, perr := strconv.ParseUint(s, 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("expected uint value: %w", perr)
		}

		return n, nil
	}

	var n uint64
	if err := json.Unmarshal(v.literal, &n); err != nil {
		return 0, fmt.Errorf("expected uint value: %w", err)
	}

	return n, nil
}

// MarshalJSON implements json.Marshaler for QueryFilter.
func (x *QueryFilter) MarshalJSON() ([]byte, error) {
	if x == nil {
		return []byte("null"), nil
	}

	node, err := marshalNode(x)
	if err != nil {
		return nil, err
	}

	return json.Marshal(node)
}

// marshalNode converts a QueryFilter to its DSL representation (a
// map[string]any with a single top-level operator key).
func marshalNode(x *QueryFilter) (map[string]any, error) {
	switch f := x.GetFilter().(type) {
	case *QueryFilter_And:
		items, err := marshalNodes(f.And.GetFilters())
		if err != nil {
			return nil, err
		}

		return map[string]any{opAnd: items}, nil
	case *QueryFilter_Or:
		items, err := marshalNodes(f.Or.GetFilters())
		if err != nil {
			return nil, err
		}

		return map[string]any{opOr: items}, nil
	case *QueryFilter_Not:
		inner, err := marshalNode(f.Not.GetFilter())
		if err != nil {
			return nil, err
		}

		return map[string]any{opNot: inner}, nil
	case nil:
		return nil, errors.New("query filter: no condition set")
	default:
		return marshalLeaf(x)
	}
}

func marshalNodes(filters []*QueryFilter) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(filters))
	for i, f := range filters {
		node, err := marshalNode(f)
		if err != nil {
			return nil, fmt.Errorf("filter[%d]: %w", i, err)
		}
		out = append(out, node)
	}

	return out, nil
}

// kv builds a single-key operator map {op: {field: value}}.
func kv(op, field string, value jsonValue) map[string]any {
	return map[string]any{op: map[string]any{field: value}}
}

// marshalLeaf converts a leaf QueryFilter to its DSL representation. A leaf may
// expand to an $and (closed range).
func marshalLeaf(x *QueryFilter) (map[string]any, error) {
	switch f := x.GetFilter().(type) {
	case *QueryFilter_Field:
		return marshalFieldCondition(f.Field)
	case *QueryFilter_Address:
		return marshalAddressMatch(f.Address)
	case *QueryFilter_Reference:
		return marshalStringCond(opMatch, "reference", f.Reference.GetCond())
	case *QueryFilter_Ledger:
		return marshalStringCond(opMatch, "ledger", f.Ledger.GetCond())
	case *QueryFilter_Reverted:
		v, err := literalValue(f.Reverted.GetValue())
		if err != nil {
			return nil, err
		}

		return kv(opMatch, "reverted", v), nil
	case *QueryFilter_AccountHasAsset:
		v, err := literalValue(assetRefString(f.AccountHasAsset))
		if err != nil {
			return nil, err
		}

		return kv(opExists, "asset", v), nil
	case *QueryFilter_LogId:
		return marshalUintRange("logId", f.LogId.GetCond())
	case *QueryFilter_BuiltinUint:
		field, ok := txBuiltinFieldToJSON[f.BuiltinUint.GetField()]
		if !ok {
			return nil, fmt.Errorf("builtinUint: unsupported field %v", f.BuiltinUint.GetField())
		}

		return marshalUintRange(field, f.BuiltinUint.GetCond())
	case *QueryFilter_LogBuiltinUint:
		switch f.LogBuiltinUint.GetField() {
		case LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
			return marshalUintRange("date", f.LogBuiltinUint.GetCond())
		default:
			return nil, fmt.Errorf("logBuiltinUint: unsupported field %v", f.LogBuiltinUint.GetField())
		}
	default:
		return nil, fmt.Errorf("query filter: unhandled leaf %T", f)
	}
}

func assetRefString(c *AccountHasAssetCondition) string {
	if c.GetPrecision() == 0 {
		return c.GetAssetBase()
	}

	return fmt.Sprintf("%s/%d", c.GetAssetBase(), c.GetPrecision())
}

func parseAssetRef(ref string) (string, uint32, error) {
	for i := len(ref) - 1; i >= 0; i-- {
		if ref[i] == '/' {
			p, err := strconv.ParseUint(ref[i+1:], 10, 32)
			if err != nil {
				return "", 0, fmt.Errorf("asset precision: %w", err)
			}

			return ref[:i], uint32(p), nil
		}
	}

	return ref, 0, nil
}

// --- string / bool leaf --------------------------------------------------

func marshalStringCond(op, field string, cond *StringCondition) (map[string]any, error) {
	v, err := stringCondValue(cond)
	if err != nil {
		return nil, err
	}

	return kv(op, field, v), nil
}

func stringCondValue(cond *StringCondition) (jsonValue, error) {
	switch c := cond.GetValue().(type) {
	case *StringCondition_Hardcoded:
		return literalValue(c.Hardcoded)
	case *StringCondition_Param:
		return paramValue(c.Param), nil
	default:
		return jsonValue{}, errors.New("string condition: no value set")
	}
}

func stringCondFromValue(v jsonValue) (*StringCondition, error) {
	if v.isParam() {
		return &StringCondition{Value: &StringCondition_Param{Param: v.param}}, nil
	}
	s, err := v.asString()
	if err != nil {
		return nil, err
	}

	return &StringCondition{Value: &StringCondition_Hardcoded{Hardcoded: s}}, nil
}

// --- FieldCondition (metadata) -------------------------------------------

func marshalFieldCondition(fc *FieldCondition) (map[string]any, error) {
	key := metadataKey(fc.GetField().GetMetadata())

	switch c := fc.GetCondition().(type) {
	case *FieldCondition_StringCond:
		return marshalStringCond(opMatch, key, c.StringCond)
	case *FieldCondition_BoolCond:
		return marshalBoolCond(key, c.BoolCond)
	case *FieldCondition_ExistsCond:
		// metadata existence uses {"$exists": {"metadata": "<key>"}} (v2 shape),
		// not the metadata[<key>] field form.
		v, err := literalValue(fc.GetField().GetMetadata())
		if err != nil {
			return nil, err
		}

		return kv(opExists, "metadata", v), nil
	case *FieldCondition_IntCond:
		return marshalIntRange(key, c.IntCond)
	case *FieldCondition_UintCond:
		return marshalUintRange(key, c.UintCond)
	default:
		return nil, errors.New("field: no condition set")
	}
}

func marshalBoolCond(field string, cond *BoolCondition) (map[string]any, error) {
	switch c := cond.GetValue().(type) {
	case *BoolCondition_Hardcoded:
		v, err := literalValue(c.Hardcoded)
		if err != nil {
			return nil, err
		}

		return kv(opMatch, field, v), nil
	case *BoolCondition_Param:
		return kv(opMatch, field, paramValue(c.Param)), nil
	default:
		return nil, errors.New("bool condition: no value set")
	}
}

// --- ranges (int / uint) -------------------------------------------------

// rangeBound is a single {op: {field: value}} for a range endpoint.
func lowerOp(exclusive bool) string {
	if exclusive {
		return opGt
	}

	return opGte
}

func upperOp(exclusive bool) string {
	if exclusive {
		return opLt
	}

	return opLte
}

// wrapRange combines zero, one, or two bound clauses. Two bounds become an
// $and; a single bound is emitted directly. An empty range is an error.
func wrapRange(field string, clauses []map[string]any) (map[string]any, error) {
	switch len(clauses) {
	case 0:
		return nil, fmt.Errorf("%s: range has no bound", field)
	case 1:
		return clauses[0], nil
	default:
		items := make([]any, len(clauses))
		for i, c := range clauses {
			items[i] = c
		}

		return map[string]any{opAnd: items}, nil
	}
}

func marshalIntRange(field string, cond *IntCondition) (map[string]any, error) {
	var clauses []map[string]any

	if cond.GetParamMin() != "" {
		clauses = append(clauses, kv(lowerOp(cond.GetMinExclusive()), field, paramValue(cond.GetParamMin())))
	} else if cond.Min != nil {
		v, err := literalValue(cond.GetMin())
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, kv(lowerOp(cond.GetMinExclusive()), field, v))
	}

	if cond.GetParamMax() != "" {
		clauses = append(clauses, kv(upperOp(cond.GetMaxExclusive()), field, paramValue(cond.GetParamMax())))
	} else if cond.Max != nil {
		v, err := literalValue(cond.GetMax())
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, kv(upperOp(cond.GetMaxExclusive()), field, v))
	}

	return wrapRange(field, clauses)
}

func marshalUintRange(field string, cond *UintCondition) (map[string]any, error) {
	var clauses []map[string]any

	if cond.GetParamMin() != "" {
		clauses = append(clauses, kv(lowerOp(cond.GetMinExclusive()), field, paramValue(cond.GetParamMin())))
	} else if cond.Min != nil {
		// uint64 emitted as a decimal string to stay lossless above 2^53.
		v, err := literalValue(strconv.FormatUint(cond.GetMin(), 10))
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, kv(lowerOp(cond.GetMinExclusive()), field, v))
	}

	if cond.GetParamMax() != "" {
		clauses = append(clauses, kv(upperOp(cond.GetMaxExclusive()), field, paramValue(cond.GetParamMax())))
	} else if cond.Max != nil {
		v, err := literalValue(strconv.FormatUint(cond.GetMax(), 10))
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, kv(upperOp(cond.GetMaxExclusive()), field, v))
	}

	return wrapRange(field, clauses)
}

// --- AddressMatch --------------------------------------------------------

func marshalAddressMatch(am *AddressMatch) (map[string]any, error) {
	field := "address"
	switch am.GetRole() {
	case AddressRole_ADDRESS_ROLE_SOURCE:
		field = "source"
	case AddressRole_ADDRESS_ROLE_DESTINATION:
		field = "destination"
	case AddressRole_ADDRESS_ROLE_ANY:
		field = "address"
	}

	switch m := am.GetMatch().(type) {
	case *AddressMatch_HardcodedPrefix:
		// A prefix whose value already ends in ":" round-trips via $match (the
		// trailing colon is the DSL prefix marker). A prefix WITHOUT a trailing
		// colon (a byte-level prefix) would be indistinguishable from an exact
		// match under $match, so it is emitted via $like with a literal value to
		// preserve prefix intent losslessly.
		if m.HardcodedPrefix != "" && m.HardcodedPrefix[len(m.HardcodedPrefix)-1] == ':' {
			v, err := literalValue(m.HardcodedPrefix)
			if err != nil {
				return nil, err
			}

			return kv(opMatch, field, v), nil
		}
		v, err := literalValue(m.HardcodedPrefix)
		if err != nil {
			return nil, err
		}

		return kv(opLike, field, v), nil
	case *AddressMatch_HardcodedExact:
		v, err := literalValue(m.HardcodedExact)
		if err != nil {
			return nil, err
		}

		return kv(opMatch, field, v), nil
	case *AddressMatch_ParamPrefix:
		// A parameterised prefix cannot carry the trailing ":" marker on the
		// literal, so it is encoded as a $like with a param (prefix semantics
		// resolved at execution time from the param value).
		return kv(opLike, field, paramValue(m.ParamPrefix)), nil
	case *AddressMatch_ParamExact:
		return kv(opMatch, field, paramValue(m.ParamExact)), nil
	default:
		return nil, errors.New("address: no match set")
	}
}

// ============================================================================
// UnmarshalJSON
// ============================================================================

// UnmarshalJSON implements json.Unmarshaler for QueryFilter.
func (x *QueryFilter) UnmarshalJSON(data []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	op, raw, err := singleOperator(m)
	if err != nil {
		return err
	}

	filter, err := decodeExpression(op, raw)
	if err != nil {
		return err
	}

	x.Filter = filter.GetFilter()

	return nil
}

// singleOperator returns the single top-level operator key and its raw value.
func singleOperator(m map[string]json.RawMessage) (string, json.RawMessage, error) {
	switch len(m) {
	case 0:
		return "", nil, errors.New("query filter: empty object (expected a single operator key)")
	case 1:
		for k, v := range m {
			return k, v, nil
		}
	}

	return "", nil, errors.New("query filter: expected exactly one operator key")
}

// decodeExpression decodes one DSL node into a QueryFilter.
func decodeExpression(op string, raw json.RawMessage) (*QueryFilter, error) {
	switch op {
	case opAnd, opOr:
		return decodeCombinator(op, raw)
	case opNot:
		inner := &QueryFilter{}
		if err := json.Unmarshal(raw, inner); err != nil {
			return nil, fmt.Errorf("$not: %w", err)
		}

		return &QueryFilter{Filter: &QueryFilter_Not{Not: &NotFilter{Filter: inner}}}, nil
	case opMatch, opGt, opGte, opLt, opLte, opExists, opIn, opLike:
		return decodeLeaf(op, raw)
	default:
		return nil, fmt.Errorf("query filter: unknown operator %q", op)
	}
}

func decodeCombinator(op string, raw json.RawMessage) (*QueryFilter, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("%s: expected an array: %w", op, err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("%s: must contain at least one filter", op)
	}

	// An $and of exactly two same-field, complementary range bounds folds into a
	// single range proto condition (the inverse of marshalXxxRange). This keeps
	// a closed range `{"$and":[{"$gte":{"timestamp":1}},{"$lte":{"timestamp":9}}]}`
	// round-tripping to one BuiltinUintCondition.
	if op == opAnd {
		if folded, ok, err := foldRangeAnd(items); err != nil {
			return nil, err
		} else if ok {
			return folded, nil
		}
	}

	filters := make([]*QueryFilter, 0, len(items))
	for i, it := range items {
		sub := &QueryFilter{}
		if err := json.Unmarshal(it, sub); err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", op, i, err)
		}
		filters = append(filters, sub)
	}

	if op == opAnd {
		return &QueryFilter{Filter: &QueryFilter_And{And: &AndFilter{Filters: filters}}}, nil
	}

	return &QueryFilter{Filter: &QueryFilter_Or{Or: &OrFilter{Filters: filters}}}, nil
}

// foldRangeAnd detects an $and array of exactly two range-bound leaves on the
// same field with complementary directions (one lower, one upper) and folds them
// into a single range condition. ok=false leaves the $and intact.
func foldRangeAnd(items []json.RawMessage) (*QueryFilter, bool, error) {
	if len(items) != 2 {
		return nil, false, nil
	}

	bounds := make([]bound, 0, 2)
	for _, it := range items {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(it, &m); err != nil {
			return nil, false, nil
		}
		op, body, err := singleOperator(m)
		if err != nil {
			return nil, false, nil
		}
		if op != opGt && op != opGte && op != opLt && op != opLte {
			return nil, false, nil
		}
		field, value, err := leafKV(op, body)
		if err != nil {
			return nil, false, err
		}
		bounds = append(bounds, bound{op: op, field: field, value: value})
	}

	// same field, one lower + one upper
	if bounds[0].field != bounds[1].field {
		return nil, false, nil
	}
	if bounds[0].isLower() == bounds[1].isLower() {
		return nil, false, nil
	}

	sortBounds(bounds)
	qf, err := buildRange(bounds[0].field, bounds)
	if err != nil {
		return nil, false, err
	}

	return qf, true, nil
}

// leafKV decodes the {field: value} body of a leaf operator into its single
// field name and value.
func leafKV(op string, raw json.RawMessage) (string, jsonValue, error) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", jsonValue{}, fmt.Errorf("%s: %w", op, err)
	}
	if len(body) != 1 {
		return "", jsonValue{}, fmt.Errorf("%s: expected exactly one field", op)
	}

	var (
		field  string
		rawVal json.RawMessage
	)
	for field, rawVal = range body {
	}

	var v jsonValue
	if err := json.Unmarshal(rawVal, &v); err != nil {
		return "", jsonValue{}, fmt.Errorf("%s.%s: %w", op, field, err)
	}

	return field, v, nil
}

func decodeLeaf(op string, raw json.RawMessage) (*QueryFilter, error) {
	field, value, err := leafKV(op, raw)
	if err != nil {
		return nil, err
	}

	switch op {
	case opExists:
		return decodeExists(field, value)
	case opMatch:
		return decodeMatch(field, value)
	case opGt, opGte, opLt, opLte:
		return decodeSingleBound(op, field, value)
	case opLike:
		return decodeLike(field, value)
	case opIn:
		return nil, errors.New("$in: not supported for this filter surface")
	default:
		return nil, fmt.Errorf("unhandled operator %q", op)
	}
}

func decodeExists(field string, value jsonValue) (*QueryFilter, error) {
	if value.isParam() {
		return nil, errors.New("$exists: value must be a literal key, not a param")
	}
	switch field {
	case "metadata":
		key, err := value.asString()
		if err != nil {
			return nil, err
		}
		if key == "" {
			return nil, errors.New("$exists metadata: key is required")
		}

		return &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
			Field:     &FieldRef{Metadata: key},
			Condition: &FieldCondition_ExistsCond{ExistsCond: &ExistsCondition{}},
		}}}, nil
	case "asset":
		ref, err := value.asString()
		if err != nil {
			return nil, err
		}
		if ref == "" {
			return nil, errors.New("$exists asset: assetRef is required")
		}
		base, precision, perr := parseAssetRef(ref)
		if perr != nil {
			return nil, perr
		}

		return &QueryFilter{Filter: &QueryFilter_AccountHasAsset{AccountHasAsset: &AccountHasAssetCondition{
			AssetBase: base, Precision: precision,
		}}}, nil
	default:
		return nil, fmt.Errorf("$exists: unsupported field %q (use metadata or asset)", field)
	}
}

func decodeMatch(field string, value jsonValue) (*QueryFilter, error) {
	switch field {
	case "address", "source", "destination":
		return decodeAddressMatch(field, value)
	case "reference":
		sc, err := stringCondFromValue(value)
		if err != nil {
			return nil, err
		}

		return &QueryFilter{Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{Cond: sc}}}, nil
	case "ledger":
		sc, err := stringCondFromValue(value)
		if err != nil {
			return nil, err
		}

		return &QueryFilter{Filter: &QueryFilter_Ledger{Ledger: &LedgerCondition{Cond: sc}}}, nil
	case "reverted":
		if value.isParam() {
			return nil, errors.New("reverted: param not supported")
		}
		b, err := value.asBool()
		if err != nil {
			return nil, err
		}

		return &QueryFilter{Filter: &QueryFilter_Reverted{Reverted: &RevertedCondition{Value: b}}}, nil
	default:
		if key, ok := parseMetadataKey(field); ok {
			return decodeMetadataMatch(key, value)
		}

		return nil, fmt.Errorf("$match: unsupported field %q", field)
	}
}

func decodeAddressMatch(field string, value jsonValue) (*QueryFilter, error) {
	am := &AddressMatch{}
	switch field {
	case "source":
		am.Role = AddressRole_ADDRESS_ROLE_SOURCE
	case "destination":
		am.Role = AddressRole_ADDRESS_ROLE_DESTINATION
	}

	if value.isParam() {
		// $match with a param is an exact param match ($like carries prefix).
		am.Match = &AddressMatch_ParamExact{ParamExact: value.param}

		return &QueryFilter{Filter: &QueryFilter_Address{Address: am}}, nil
	}

	s, err := value.asString()
	if err != nil {
		return nil, err
	}
	if s != "" && s[len(s)-1] == ':' {
		am.Match = &AddressMatch_HardcodedPrefix{HardcodedPrefix: s}
	} else {
		am.Match = &AddressMatch_HardcodedExact{HardcodedExact: s}
	}

	return &QueryFilter{Filter: &QueryFilter_Address{Address: am}}, nil
}

func decodeLike(field string, value jsonValue) (*QueryFilter, error) {
	// $like carries an address prefix — either a literal (hardcoded byte-level
	// prefix that cannot be expressed via the trailing-":" $match convention) or
	// a parameterised prefix resolved at execution time.
	if field != "address" && field != "source" && field != "destination" {
		return nil, fmt.Errorf("$like: unsupported field %q", field)
	}

	am := &AddressMatch{}
	if value.isParam() {
		am.Match = &AddressMatch_ParamPrefix{ParamPrefix: value.param}
	} else {
		s, err := value.asString()
		if err != nil {
			return nil, fmt.Errorf("$like: %w", err)
		}
		am.Match = &AddressMatch_HardcodedPrefix{HardcodedPrefix: s}
	}

	switch field {
	case "source":
		am.Role = AddressRole_ADDRESS_ROLE_SOURCE
	case "destination":
		am.Role = AddressRole_ADDRESS_ROLE_DESTINATION
	}

	return &QueryFilter{Filter: &QueryFilter_Address{Address: am}}, nil
}

func decodeMetadataMatch(key string, value jsonValue) (*QueryFilter, error) {
	fc := &FieldCondition{Field: &FieldRef{Metadata: key}}

	if value.isParam() {
		fc.Condition = &FieldCondition_StringCond{StringCond: &StringCondition{Value: &StringCondition_Param{Param: value.param}}}

		return &QueryFilter{Filter: &QueryFilter_Field{Field: fc}}, nil
	}

	// Distinguish bool from string by the literal shape.
	var b bool
	if err := json.Unmarshal(value.literal, &b); err == nil {
		fc.Condition = &FieldCondition_BoolCond{BoolCond: &BoolCondition{Value: &BoolCondition_Hardcoded{Hardcoded: b}}}

		return &QueryFilter{Filter: &QueryFilter_Field{Field: fc}}, nil
	}

	s, err := value.asString()
	if err != nil {
		return nil, fmt.Errorf("metadata[%s]: %w", key, err)
	}
	fc.Condition = &FieldCondition_StringCond{StringCond: &StringCondition{Value: &StringCondition_Hardcoded{Hardcoded: s}}}

	return &QueryFilter{Filter: &QueryFilter_Field{Field: fc}}, nil
}

// decodeSingleBound decodes a lone $gt/$gte/$lt/$lte into a range proto
// condition with only one bound set.
func decodeSingleBound(op, field string, value jsonValue) (*QueryFilter, error) {
	b := bound{op: op, field: field, value: value}

	return buildRange(field, []bound{b})
}

// bound is a decoded range endpoint.
type bound struct {
	op    string
	field string
	value jsonValue
}

func (b bound) isLower() bool     { return b.op == opGt || b.op == opGte }
func (b bound) isExclusive() bool { return b.op == opGt || b.op == opLt }

// buildRange assembles one or two same-field bounds into the matching range
// proto condition, dispatching on the field name.
func buildRange(field string, bounds []bound) (*QueryFilter, error) {
	switch field {
	case "id", "timestamp", "insertedAt", "revertedAt":
		uc, err := uintConditionFromBounds(bounds)
		if err != nil {
			return nil, err
		}

		return &QueryFilter{Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{
			Field: txBuiltinFieldFromJSON[field], Cond: uc,
		}}}, nil
	case "logId":
		uc, err := uintConditionFromBounds(bounds)
		if err != nil {
			return nil, err
		}

		return &QueryFilter{Filter: &QueryFilter_LogId{LogId: &LogIdCondition{Cond: uc}}}, nil
	case "date":
		uc, err := uintConditionFromBounds(bounds)
		if err != nil {
			return nil, err
		}

		return &QueryFilter{Filter: &QueryFilter_LogBuiltinUint{LogBuiltinUint: &LogBuiltinUintCondition{
			Field: LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, Cond: uc,
		}}}, nil
	default:
		if key, ok := parseMetadataKey(field); ok {
			// Metadata numeric ranges may be signed (IntCondition) or unsigned
			// (UintCondition). We keep them distinguishable — and thus lossless —
			// the same way the encoder does: unsigned bounds are decimal strings,
			// signed bounds are JSON numbers. A param-only range carries no
			// literal to inspect and defaults to the signed form.
			if boundsAreUnsigned(bounds) {
				uc, err := uintConditionFromBounds(bounds)
				if err != nil {
					return nil, err
				}

				return &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
					Field:     &FieldRef{Metadata: key},
					Condition: &FieldCondition_UintCond{UintCond: uc},
				}}}, nil
			}

			ic, err := intConditionFromBounds(bounds)
			if err != nil {
				return nil, err
			}

			return &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{
				Field:     &FieldRef{Metadata: key},
				Condition: &FieldCondition_IntCond{IntCond: ic},
			}}}, nil
		}

		return nil, fmt.Errorf("range: unsupported field %q", field)
	}
}

// boundsAreUnsigned reports whether the literal bound values are JSON strings
// (the unsigned encoding). Returns false when all bounds are params (no literal
// to inspect) so the signed form is chosen by default.
func boundsAreUnsigned(bounds []bound) bool {
	sawLiteral := false
	for _, b := range bounds {
		if b.value.isParam() {
			continue
		}
		sawLiteral = true
		var s string
		if err := json.Unmarshal(b.value.literal, &s); err != nil {
			return false
		}
	}

	return sawLiteral
}

func intConditionFromBounds(bounds []bound) (*IntCondition, error) {
	ic := &IntCondition{}
	for _, b := range bounds {
		if b.isLower() {
			ic.MinExclusive = b.isExclusive()
			if b.value.isParam() {
				ic.ParamMin = b.value.param
			} else {
				n, err := b.value.asInt64()
				if err != nil {
					return nil, err
				}
				v := n
				ic.Min = &v
			}
		} else {
			ic.MaxExclusive = b.isExclusive()
			if b.value.isParam() {
				ic.ParamMax = b.value.param
			} else {
				n, err := b.value.asInt64()
				if err != nil {
					return nil, err
				}
				v := n
				ic.Max = &v
			}
		}
	}

	return ic, nil
}

func uintConditionFromBounds(bounds []bound) (*UintCondition, error) {
	uc := &UintCondition{}
	for _, b := range bounds {
		if b.isLower() {
			uc.MinExclusive = b.isExclusive()
			if b.value.isParam() {
				uc.ParamMin = b.value.param
			} else {
				n, err := b.value.asUint64()
				if err != nil {
					return nil, err
				}
				v := n
				uc.Min = &v
			}
		} else {
			uc.MaxExclusive = b.isExclusive()
			if b.value.isParam() {
				uc.ParamMax = b.value.param
			} else {
				n, err := b.value.asUint64()
				if err != nil {
					return nil, err
				}
				v := n
				uc.Max = &v
			}
		}
	}

	return uc, nil
}

// sortBoundsForStableProto keeps a deterministic ordering (lower then upper).
func sortBounds(bounds []bound) {
	sort.SliceStable(bounds, func(i, j int) bool {
		return bounds[i].isLower() && !bounds[j].isLower()
	})
}
