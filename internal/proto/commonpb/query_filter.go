package commonpb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// QueryFilter has a hand-written JSON codec (this file) instead of relying on
// protojson. protojson would leak the protobuf-internal oneof/wrapper names
// onto the public REST surface: the top-level oneof would surface as
// `{"and":{"filters":[...]}}` / `{"not":{"filter":{...}}}` and leaf conditions
// as `{"field":{"field":{"metadata":"x"},"intCond":{...}}}`, none of which is a
// contract we want to expose or that a hand-written OpenAPI schema can describe
// cleanly. The canonical public shape is:
//
//	combinators (recursive):
//	  {"and": [QueryFilter, ...]}
//	  {"or":  [QueryFilter, ...]}
//	  {"not": QueryFilter}
//	leaf:
//	  {"match": <Condition>}
//
// where <Condition> is a tagged union discriminated by a "type" field. The
// supported types map 1:1 to the proto oneof arms:
//
//	{"type":"field","metadata":"k","condition":<FieldValueCondition>}
//	{"type":"address","operator":"prefix"|"exact","value":"..."|"param":"...","role":"any"|"source"|"destination"}
//	{"type":"reference","condition":<StringCondition>}
//	{"type":"ledger","condition":<StringCondition>}
//	{"type":"logId","condition":<UintCondition>}
//	{"type":"builtinUint","field":<txBuiltinField>,"condition":<UintCondition>}
//	{"type":"logBuiltinUint","field":<logBuiltinField>,"condition":<UintCondition>}
//	{"type":"accountHasAsset","assetBase":"USD","precision":2}
//	{"type":"reverted","value":true}
//
// A FieldValueCondition is itself a tagged union discriminated by "type":
//
//	{"type":"string", ...StringCondition}
//	{"type":"int",    ...IntCondition}
//	{"type":"uint",   ...UintCondition}
//	{"type":"bool",   ...BoolCondition}
//	{"type":"exists","includeNull":false}
//
// StringCondition / BoolCondition are {"equals":<v>} or {"param":"<name>"}.
// Int/UintCondition carry optional min/max (as JSON numbers, but uint bounds
// are emitted as strings to stay lossless past 2^53), min/maxExclusive flags,
// and paramMin/paramMax alternatives.

// enum name maps between the public JSON discriminants and the proto enums.
// Kept local so the public contract never inherits the TX_BUILTIN_INDEX_*
// proto prefixes.
var (
	// Only the fields the query compiler resolves as an unsigned-range condition
	// are exposed. The remaining TransactionBuiltinIndex values
	// (REFERENCE, ADDRESS, SOURCE_ADDRESS, DESTINATION_ADDRESS) are internal
	// index kinds used by the address/reference condition compilers, NOT uint
	// range fields — compileBuiltinUintCondition rejects them, so exposing them
	// here would document valid-looking filters that fail at execution.
	txBuiltinFieldToJSON = map[TransactionBuiltinIndex]string{
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:          "id",
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:   "timestamp",
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT: "insertedAt",
		TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT: "revertedAt",
	}
	txBuiltinFieldFromJSON = invertEnumMap(txBuiltinFieldToJSON)

	logBuiltinFieldToJSON = map[LogBuiltinIndex]string{
		LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE: "date",
	}
	logBuiltinFieldFromJSON = invertEnumMap(logBuiltinFieldToJSON)

	addressRoleToJSON = map[AddressRole]string{
		AddressRole_ADDRESS_ROLE_ANY:         "any",
		AddressRole_ADDRESS_ROLE_SOURCE:      "source",
		AddressRole_ADDRESS_ROLE_DESTINATION: "destination",
	}
	addressRoleFromJSON = invertEnumMap(addressRoleToJSON)
)

func invertEnumMap[E comparable](m map[E]string) map[string]E {
	out := make(map[string]E, len(m))
	for k, v := range m {
		out[v] = k
	}

	return out
}

// MarshalJSON implements json.Marshaler for QueryFilter.
func (x *QueryFilter) MarshalJSON() ([]byte, error) {
	if x == nil {
		return []byte("null"), nil
	}

	switch f := x.GetFilter().(type) {
	case *QueryFilter_And:
		return json.Marshal(struct {
			And []*QueryFilter `json:"and"`
		}{And: f.And.GetFilters()})
	case *QueryFilter_Or:
		return json.Marshal(struct {
			Or []*QueryFilter `json:"or"`
		}{Or: f.Or.GetFilters()})
	case *QueryFilter_Not:
		return json.Marshal(struct {
			Not *QueryFilter `json:"not"`
		}{Not: f.Not.GetFilter()})
	case *QueryFilter_Field, *QueryFilter_Address, *QueryFilter_Reference,
		*QueryFilter_BuiltinUint, *QueryFilter_Ledger, *QueryFilter_LogId,
		*QueryFilter_LogBuiltinUint, *QueryFilter_AccountHasAsset,
		*QueryFilter_Reverted:
		cond, err := marshalCondition(x)
		if err != nil {
			return nil, err
		}

		return json.Marshal(struct {
			Match json.RawMessage `json:"match"`
		}{Match: cond})
	case nil:
		return nil, errors.New("query filter: no condition set")
	default:
		// Impossible by design: the proto oneof has exactly the arms handled
		// above. A new arm added to the proto without extending this codec must
		// fail loudly rather than silently drop the filter.
		return nil, fmt.Errorf("query filter: unhandled filter variant %T", f)
	}
}

// UnmarshalJSON implements json.Unmarshaler for QueryFilter.
func (x *QueryFilter) UnmarshalJSON(data []byte) error {
	var env struct {
		And   []json.RawMessage `json:"and"`
		Or    []json.RawMessage `json:"or"`
		Not   json.RawMessage   `json:"not"`
		Match json.RawMessage   `json:"match"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}

	set := 0
	if env.And != nil {
		set++
	}
	if env.Or != nil {
		set++
	}
	if len(env.Not) != 0 {
		set++
	}
	if len(env.Match) != 0 {
		set++
	}

	switch {
	case set == 0:
		return errors.New("query filter must set exactly one of: and, or, not, match")
	case set > 1:
		return fmt.Errorf("query filter must set exactly one of: and, or, not, match (found %d)", set)
	}

	switch {
	case env.And != nil:
		filters, err := unmarshalFilterList("and", env.And)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_And{And: &AndFilter{Filters: filters}}
	case env.Or != nil:
		filters, err := unmarshalFilterList("or", env.Or)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_Or{Or: &OrFilter{Filters: filters}}
	case len(env.Not) != 0:
		inner := &QueryFilter{}
		if err := json.Unmarshal(env.Not, inner); err != nil {
			return err
		}
		x.Filter = &QueryFilter_Not{Not: &NotFilter{Filter: inner}}
	default:
		return unmarshalCondition(env.Match, x)
	}

	return nil
}

func unmarshalFilterList(combinator string, raw []json.RawMessage) ([]*QueryFilter, error) {
	// An empty combinator is ambiguous (empty AND = match-all, empty OR =
	// match-nothing) and almost always a client mistake — reject it loudly
	// rather than silently building a degenerate filter.
	if len(raw) == 0 {
		return nil, fmt.Errorf("%s: must contain at least one filter", combinator)
	}

	filters := make([]*QueryFilter, 0, len(raw))
	for i, r := range raw {
		sub := &QueryFilter{}
		if err := json.Unmarshal(r, sub); err != nil {
			return nil, fmt.Errorf("filter[%d]: %w", i, err)
		}
		filters = append(filters, sub)
	}

	return filters, nil
}

// marshalCondition emits the tagged-union <Condition> for a leaf QueryFilter.
func marshalCondition(x *QueryFilter) (json.RawMessage, error) {
	switch f := x.GetFilter().(type) {
	case *QueryFilter_Field:
		return marshalFieldCondition(f.Field)
	case *QueryFilter_Address:
		return marshalAddressMatch(f.Address)
	case *QueryFilter_Reference:
		return marshalTypedStringCond("reference", f.Reference.GetCond())
	case *QueryFilter_Ledger:
		return marshalTypedStringCond("ledger", f.Ledger.GetCond())
	case *QueryFilter_LogId:
		return marshalTypedUintCond("logId", "", f.LogId.GetCond())
	case *QueryFilter_BuiltinUint:
		field, ok := txBuiltinFieldToJSON[f.BuiltinUint.GetField()]
		if !ok {
			return nil, fmt.Errorf("builtinUint: unknown field %v", f.BuiltinUint.GetField())
		}

		return marshalTypedUintCond("builtinUint", field, f.BuiltinUint.GetCond())
	case *QueryFilter_LogBuiltinUint:
		field, ok := logBuiltinFieldToJSON[f.LogBuiltinUint.GetField()]
		if !ok {
			return nil, fmt.Errorf("logBuiltinUint: unknown field %v", f.LogBuiltinUint.GetField())
		}

		return marshalTypedUintCond("logBuiltinUint", field, f.LogBuiltinUint.GetCond())
	case *QueryFilter_AccountHasAsset:
		return json.Marshal(struct {
			Type      string `json:"type"`
			AssetBase string `json:"assetBase"`
			Precision uint32 `json:"precision,omitempty"`
		}{Type: "accountHasAsset", AssetBase: f.AccountHasAsset.GetAssetBase(), Precision: f.AccountHasAsset.GetPrecision()})
	case *QueryFilter_Reverted:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value bool   `json:"value"`
		}{Type: "reverted", Value: f.Reverted.GetValue()})
	default:
		return nil, fmt.Errorf("query filter: unhandled leaf condition %T", f)
	}
}

// unmarshalCondition parses the tagged-union <Condition> and sets the matching
// leaf oneof arm on x.
func unmarshalCondition(raw json.RawMessage, x *QueryFilter) error {
	var disc struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &disc); err != nil {
		return fmt.Errorf("match: %w", err)
	}

	switch disc.Type {
	case "field":
		fc, err := unmarshalFieldCondition(raw)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_Field{Field: fc}
	case "address":
		am, err := unmarshalAddressMatch(raw)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_Address{Address: am}
	case "reference":
		sc, err := unmarshalTypedStringCond(raw)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_Reference{Reference: &ReferenceCondition{Cond: sc}}
	case "ledger":
		sc, err := unmarshalTypedStringCond(raw)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_Ledger{Ledger: &LedgerCondition{Cond: sc}}
	case "logId":
		uc, err := unmarshalTypedUintCond(raw)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_LogId{LogId: &LogIdCondition{Cond: uc}}
	case "builtinUint":
		var f struct {
			Field string `json:"field"`
		}
		if err := json.Unmarshal(raw, &f); err != nil {
			return fmt.Errorf("builtinUint: %w", err)
		}
		field, ok := txBuiltinFieldFromJSON[f.Field]
		if !ok {
			return fmt.Errorf("builtinUint: unknown field %q", f.Field)
		}
		uc, err := unmarshalTypedUintCond(raw)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{Field: field, Cond: uc}}
	case "logBuiltinUint":
		var f struct {
			Field string `json:"field"`
		}
		if err := json.Unmarshal(raw, &f); err != nil {
			return fmt.Errorf("logBuiltinUint: %w", err)
		}
		field, ok := logBuiltinFieldFromJSON[f.Field]
		if !ok {
			return fmt.Errorf("logBuiltinUint: unknown field %q", f.Field)
		}
		uc, err := unmarshalTypedUintCond(raw)
		if err != nil {
			return err
		}
		x.Filter = &QueryFilter_LogBuiltinUint{LogBuiltinUint: &LogBuiltinUintCondition{Field: field, Cond: uc}}
	case "accountHasAsset":
		var c struct {
			AssetBase string `json:"assetBase"`
			Precision uint32 `json:"precision"`
		}
		if err := json.Unmarshal(raw, &c); err != nil {
			return fmt.Errorf("accountHasAsset: %w", err)
		}
		if c.AssetBase == "" {
			return errors.New("accountHasAsset: assetBase is required")
		}
		x.Filter = &QueryFilter_AccountHasAsset{AccountHasAsset: &AccountHasAssetCondition{AssetBase: c.AssetBase, Precision: c.Precision}}
	case "reverted":
		var c struct {
			Value *bool `json:"value"`
		}
		if err := json.Unmarshal(raw, &c); err != nil {
			return fmt.Errorf("reverted: %w", err)
		}
		// value is a required, meaningful boolean (false is a real query, not a
		// default) — reject absence rather than silently matching non-reverted.
		if c.Value == nil {
			return errors.New("reverted: value is required")
		}
		x.Filter = &QueryFilter_Reverted{Reverted: &RevertedCondition{Value: *c.Value}}
	case "":
		return errors.New("match: missing discriminator field \"type\"")
	default:
		return fmt.Errorf("match: unknown condition type %q", disc.Type)
	}

	return nil
}

func marshalTypedStringCond(typ string, cond *StringCondition) (json.RawMessage, error) {
	sc := stringCondToJSON(cond)
	sc.Type = typ

	return json.Marshal(sc)
}

func unmarshalTypedStringCond(raw json.RawMessage) (*StringCondition, error) {
	var sc jsonStringCond
	if err := json.Unmarshal(raw, &sc); err != nil {
		return nil, err
	}

	return sc.toProto()
}

func marshalTypedUintCond(typ, field string, cond *UintCondition) (json.RawMessage, error) {
	uc, err := uintCondToJSON(cond)
	if err != nil {
		return nil, err
	}
	uc.Type = typ
	uc.Field = field

	return json.Marshal(uc)
}

func unmarshalTypedUintCond(raw json.RawMessage) (*UintCondition, error) {
	var uc jsonUintCond
	if err := json.Unmarshal(raw, &uc); err != nil {
		return nil, err
	}

	return uc.toProto()
}

// --- FieldCondition -------------------------------------------------------

func marshalFieldCondition(fc *FieldCondition) (json.RawMessage, error) {
	inner, err := marshalFieldValueCondition(fc)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		Type      string          `json:"type"`
		Metadata  string          `json:"metadata"`
		Condition json.RawMessage `json:"condition"`
	}{Type: "field", Metadata: fc.GetField().GetMetadata(), Condition: inner})
}

func marshalFieldValueCondition(fc *FieldCondition) (json.RawMessage, error) {
	switch c := fc.GetCondition().(type) {
	case *FieldCondition_StringCond:
		return marshalTypedStringCond("string", c.StringCond)
	case *FieldCondition_IntCond:
		ic, err := intCondToJSON(c.IntCond)
		if err != nil {
			return nil, err
		}
		ic.Type = "int"

		return json.Marshal(ic)
	case *FieldCondition_UintCond:
		return marshalTypedUintCond("uint", "", c.UintCond)
	case *FieldCondition_BoolCond:
		return json.Marshal(boolCondToJSON(c.BoolCond))
	case *FieldCondition_ExistsCond:
		return json.Marshal(struct {
			Type        string `json:"type"`
			IncludeNull bool   `json:"includeNull,omitempty"`
		}{Type: "exists", IncludeNull: c.ExistsCond.GetIncludeNull()})
	default:
		return nil, errors.New("field: no condition set")
	}
}

func unmarshalFieldCondition(raw json.RawMessage) (*FieldCondition, error) {
	var outer struct {
		Metadata  string          `json:"metadata"`
		Condition json.RawMessage `json:"condition"`
	}
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil, fmt.Errorf("field: %w", err)
	}
	if outer.Metadata == "" {
		return nil, errors.New("field: metadata is required")
	}
	if len(outer.Condition) == 0 {
		return nil, errors.New("field: condition is required")
	}

	fc := &FieldCondition{Field: &FieldRef{Metadata: outer.Metadata}}

	var disc struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(outer.Condition, &disc); err != nil {
		return nil, fmt.Errorf("field.condition: %w", err)
	}

	switch disc.Type {
	case "string":
		sc, err := unmarshalTypedStringCond(outer.Condition)
		if err != nil {
			return nil, err
		}
		fc.Condition = &FieldCondition_StringCond{StringCond: sc}
	case "int":
		var ic jsonIntCond
		if err := json.Unmarshal(outer.Condition, &ic); err != nil {
			return nil, err
		}
		c, err := ic.toProto()
		if err != nil {
			return nil, err
		}
		fc.Condition = &FieldCondition_IntCond{IntCond: c}
	case "uint":
		uc, err := unmarshalTypedUintCond(outer.Condition)
		if err != nil {
			return nil, err
		}
		fc.Condition = &FieldCondition_UintCond{UintCond: uc}
	case "bool":
		var bc jsonBoolCond
		if err := json.Unmarshal(outer.Condition, &bc); err != nil {
			return nil, err
		}
		c, err := bc.toProto()
		if err != nil {
			return nil, err
		}
		fc.Condition = &FieldCondition_BoolCond{BoolCond: c}
	case "exists":
		var ec struct {
			IncludeNull bool `json:"includeNull"`
		}
		if err := json.Unmarshal(outer.Condition, &ec); err != nil {
			return nil, err
		}
		fc.Condition = &FieldCondition_ExistsCond{ExistsCond: &ExistsCondition{IncludeNull: ec.IncludeNull}}
	case "":
		return nil, errors.New("field.condition: missing discriminator field \"type\"")
	default:
		return nil, fmt.Errorf("field.condition: unknown type %q", disc.Type)
	}

	return fc, nil
}

// --- AddressMatch ---------------------------------------------------------

func marshalAddressMatch(am *AddressMatch) (json.RawMessage, error) {
	// value/param are pointers so that whichever variant is set is always
	// emitted, even when its string value is empty. Using `omitempty` on plain
	// strings would drop an explicit empty hardcoded value, producing a payload
	// (operator with no value/param) that the decoder correctly rejects — the
	// marshal/unmarshal round-trip must stay lossless and canonical.
	out := struct {
		Type     string  `json:"type"`
		Operator string  `json:"operator"`
		Value    *string `json:"value,omitempty"`
		Param    *string `json:"param,omitempty"`
		Role     string  `json:"role,omitempty"`
	}{Type: "address"}

	switch m := am.GetMatch().(type) {
	case *AddressMatch_HardcodedPrefix:
		out.Operator, out.Value = "prefix", &m.HardcodedPrefix
	case *AddressMatch_HardcodedExact:
		out.Operator, out.Value = "exact", &m.HardcodedExact
	case *AddressMatch_ParamPrefix:
		out.Operator, out.Param = "prefix", &m.ParamPrefix
	case *AddressMatch_ParamExact:
		out.Operator, out.Param = "exact", &m.ParamExact
	default:
		return nil, errors.New("address: no match set")
	}

	if role := am.GetRole(); role != AddressRole_ADDRESS_ROLE_ANY {
		out.Role = addressRoleToJSON[role]
	}

	return json.Marshal(out)
}

func unmarshalAddressMatch(raw json.RawMessage) (*AddressMatch, error) {
	// value/param are presence-tracked pointers so an explicit empty string
	// (a match-all hardcoded prefix, constructible via gRPC) round-trips
	// losslessly, while an omitted key is rejected — exactly one of the two must
	// be *present*, mirroring the string/bool conditions.
	var in struct {
		Operator string  `json:"operator"`
		Value    *string `json:"value"`
		Param    *string `json:"param"`
		Role     string  `json:"role"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, fmt.Errorf("address: %w", err)
	}

	switch in.Operator {
	case "prefix", "exact":
		// valid — validated further below
	case "":
		return nil, errors.New("address: operator is required (prefix or exact)")
	default:
		return nil, fmt.Errorf("address: unknown operator %q", in.Operator)
	}

	// Exactly one of value/param must be present. Absence of both is rejected
	// (an omitted value would otherwise default to a match-all empty prefix).
	if in.Value != nil && in.Param != nil {
		return nil, errors.New("address: set only one of value or param")
	}
	if in.Value == nil && in.Param == nil {
		return nil, errors.New("address: set one of value or param")
	}

	am := &AddressMatch{}
	switch {
	case in.Operator == "prefix" && in.Param != nil:
		am.Match = &AddressMatch_ParamPrefix{ParamPrefix: *in.Param}
	case in.Operator == "prefix":
		am.Match = &AddressMatch_HardcodedPrefix{HardcodedPrefix: *in.Value}
	case in.Operator == "exact" && in.Param != nil:
		am.Match = &AddressMatch_ParamExact{ParamExact: *in.Param}
	default: // exact + hardcoded value
		am.Match = &AddressMatch_HardcodedExact{HardcodedExact: *in.Value}
	}

	if in.Role != "" {
		role, ok := addressRoleFromJSON[in.Role]
		if !ok {
			return nil, fmt.Errorf("address: unknown role %q", in.Role)
		}
		am.Role = role
	}

	return am, nil
}

// --- scalar conditions ----------------------------------------------------

type jsonStringCond struct {
	Type   string  `json:"type,omitempty"`
	Equals *string `json:"equals,omitempty"`
	Param  string  `json:"param,omitempty"`
}

func stringCondToJSON(cond *StringCondition) jsonStringCond {
	out := jsonStringCond{}
	switch v := cond.GetValue().(type) {
	case *StringCondition_Hardcoded:
		s := v.Hardcoded
		out.Equals = &s
	case *StringCondition_Param:
		out.Param = v.Param
	}

	return out
}

func (c jsonStringCond) toProto() (*StringCondition, error) {
	if c.Equals != nil && c.Param != "" {
		return nil, errors.New("string condition: set only one of equals or param")
	}
	sc := &StringCondition{}
	switch {
	case c.Param != "":
		sc.Value = &StringCondition_Param{Param: c.Param}
	case c.Equals != nil:
		sc.Value = &StringCondition_Hardcoded{Hardcoded: *c.Equals}
	default:
		return nil, errors.New("string condition: set one of equals or param")
	}

	return sc, nil
}

type jsonBoolCond struct {
	Type   string `json:"type"`
	Equals *bool  `json:"equals,omitempty"`
	Param  string `json:"param,omitempty"`
}

func boolCondToJSON(cond *BoolCondition) jsonBoolCond {
	out := jsonBoolCond{Type: "bool"}
	switch v := cond.GetValue().(type) {
	case *BoolCondition_Hardcoded:
		b := v.Hardcoded
		out.Equals = &b
	case *BoolCondition_Param:
		out.Param = v.Param
	}

	return out
}

func (c jsonBoolCond) toProto() (*BoolCondition, error) {
	if c.Equals != nil && c.Param != "" {
		return nil, errors.New("bool condition: set only one of equals or param")
	}
	bc := &BoolCondition{}
	switch {
	case c.Param != "":
		bc.Value = &BoolCondition_Param{Param: c.Param}
	case c.Equals != nil:
		bc.Value = &BoolCondition_Hardcoded{Hardcoded: *c.Equals}
	default:
		return nil, errors.New("bool condition: set one of equals or param")
	}

	return bc, nil
}

type jsonIntCond struct {
	Type         string `json:"type"`
	Min          *int64 `json:"min,omitempty"`
	Max          *int64 `json:"max,omitempty"`
	MinExclusive bool   `json:"minExclusive,omitempty"`
	MaxExclusive bool   `json:"maxExclusive,omitempty"`
	ParamMin     string `json:"paramMin,omitempty"`
	ParamMax     string `json:"paramMax,omitempty"`
}

func intCondToJSON(cond *IntCondition) (jsonIntCond, error) {
	out := jsonIntCond{
		Type:         "int",
		MinExclusive: cond.GetMinExclusive(),
		MaxExclusive: cond.GetMaxExclusive(),
		ParamMin:     cond.GetParamMin(),
		ParamMax:     cond.GetParamMax(),
	}
	if cond.Min != nil {
		v := cond.GetMin()
		out.Min = &v
	}
	if cond.Max != nil {
		v := cond.GetMax()
		out.Max = &v
	}

	return out, nil
}

func (c jsonIntCond) toProto() (*IntCondition, error) {
	ic := &IntCondition{
		MinExclusive: c.MinExclusive,
		MaxExclusive: c.MaxExclusive,
		ParamMin:     c.ParamMin,
		ParamMax:     c.ParamMax,
	}
	if c.Min != nil {
		v := *c.Min
		ic.Min = &v
	}
	if c.Max != nil {
		v := *c.Max
		ic.Max = &v
	}

	return ic, nil
}

// jsonUintCond mirrors UintCondition. fixed64 bounds are encoded as JSON
// strings to stay lossless above 2^53, matching how uint64 is carried
// elsewhere on the wire.
type jsonUintCond struct {
	Type         string `json:"type"`
	Field        string `json:"field,omitempty"`
	Min          string `json:"min,omitempty"`
	Max          string `json:"max,omitempty"`
	MinExclusive bool   `json:"minExclusive,omitempty"`
	MaxExclusive bool   `json:"maxExclusive,omitempty"`
	ParamMin     string `json:"paramMin,omitempty"`
	ParamMax     string `json:"paramMax,omitempty"`
}

func uintCondToJSON(cond *UintCondition) (jsonUintCond, error) {
	out := jsonUintCond{
		MinExclusive: cond.GetMinExclusive(),
		MaxExclusive: cond.GetMaxExclusive(),
		ParamMin:     cond.GetParamMin(),
		ParamMax:     cond.GetParamMax(),
	}
	if cond.Min != nil {
		out.Min = strconv.FormatUint(cond.GetMin(), 10)
	}
	if cond.Max != nil {
		out.Max = strconv.FormatUint(cond.GetMax(), 10)
	}

	return out, nil
}

func (c jsonUintCond) toProto() (*UintCondition, error) {
	uc := &UintCondition{
		MinExclusive: c.MinExclusive,
		MaxExclusive: c.MaxExclusive,
		ParamMin:     c.ParamMin,
		ParamMax:     c.ParamMax,
	}
	if c.Min != "" {
		v, err := strconv.ParseUint(c.Min, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("uint condition: min: %w", err)
		}
		uc.Min = &v
	}
	if c.Max != "" {
		v, err := strconv.ParseUint(c.Max, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("uint condition: max: %w", err)
		}
		uc.Max = &v
	}

	return uc, nil
}
