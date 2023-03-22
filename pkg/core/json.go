package core

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type ValueJSON struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func TypeFromName(name string) (Type, bool) {
	switch name {
	case "account":
		return TypeAccount, true
	case "asset":
		return TypeAsset, true
	case "number":
		return TypeNumber, true
	case "portion":
		return TypePortion, true
	case "monetary":
		return TypeMonetary, true
	default:
		return 0, false
	}
}

func NewValueFromTypedJSON(rawInput json.RawMessage) (Value, error) {
	var input ValueJSON
	if err := json.Unmarshal(rawInput, &input); err != nil {
		return nil, err
	}

	typ, ok := TypeFromName(input.Type)
	if !ok {
		return nil, fmt.Errorf("unknown type: %v", input.Type)
	}

	return NewValueFromJSON(typ, input.Value)
}

func NewValueFromJSON(typ Type, data json.RawMessage) (Value, error) {
	var value Value
	switch typ {
	case TypeAccount:
		var account AccountAddress
		if err := json.Unmarshal(data, &account); err != nil {
			return nil, err
		}
		if err := ParseAccountAddress(account); err != nil {
			return nil, errors.Wrapf(err, "value %s", string(account))
		}
		value = account
	case TypeAsset:
		var asset Asset
		if err := json.Unmarshal(data, &asset); err != nil {
			return nil, err
		}
		if err := ParseAsset(asset); err != nil {
			return nil, errors.Wrapf(err, "value %s", asset.String())
		}
		value = asset
	case TypeNumber:
		var number Number
		if err := json.Unmarshal(data, &number); err != nil {
			return nil, err
		}
		value = number
	case TypeMonetary:
		var monTmp struct {
			Asset  string       `json:"asset"`
			Amount *MonetaryInt `json:"amount"`
		}
		if err := json.Unmarshal(data, &monTmp); err != nil {
			return nil, err
		}
		mon := Monetary{
			Asset:  Asset(monTmp.Asset),
			Amount: monTmp.Amount,
		}
		if err := ParseMonetary(mon); err != nil {
			return nil, errors.Wrapf(err, "value %s", mon.String())
		}
		value = mon
	case TypePortion:
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, err
		}
		res, err := ParsePortionSpecific(s)
		if err != nil {
			return nil, err
		}
		value = *res
	case TypeString:
		var s String
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, err
		}
		value = s
	default:
		return nil, fmt.Errorf("invalid type '%v'", typ)
	}

	return value, nil
}

func NewJSONFromValue(value Value) (any, error) {
	switch value.GetType() {
	case TypeAccount:
		return string(value.(AccountAddress)), nil
	case TypeAsset:
		return string(value.(Asset)), nil
	case TypeString:
		return string(value.(String)), nil
	case TypeNumber:
		return value.(*MonetaryInt).String(), nil
	case TypeMonetary:
		return value, nil

	case TypePortion:
		return value.(Portion).String(), nil
	default:
		return nil, fmt.Errorf("invalid type '%v'", value.GetType())
	}
}
