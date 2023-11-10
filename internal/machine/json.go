package machine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type ValueJSON struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func NewValueFromString(typ Type, data string) (Value, error) {
	var value Value
	switch typ {
	case TypeAccount:
		if err := ValidateAccountAddress(AccountAddress(data)); err != nil {
			return nil, errors.Wrapf(err, "value %s", data)
		}
		value = AccountAddress(data)
	case TypeAsset:
		if err := ValidateAsset(Asset(data)); err != nil {
			return nil, errors.Wrapf(err, "value %s", data)
		}
		value = Asset(data)
	case TypeNumber:
		var number Number
		if err := json.Unmarshal([]byte(data), &number); err != nil {
			return nil, err
		}
		value = number
	case TypeMonetary:
		parts := strings.SplitN(data, " ", 2)
		if len(parts) != 2 {
			return nil, errors.New("monetary must have two parts")
		}
		mi, err := ParseMonetaryInt(parts[1])
		if err != nil {
			return nil, err
		}
		mon := Monetary{
			Asset:  Asset(parts[0]),
			Amount: mi,
		}
		if err := ParseMonetary(mon); err != nil {
			return nil, errors.Wrapf(err, "value %s", mon.String())
		}
		value = mon
	case TypePortion:
		res, err := ParsePortionSpecific(data)
		if err != nil {
			return nil, err
		}
		value = *res
	case TypeString:
		value = String(data)
	default:
		return nil, fmt.Errorf("invalid type '%v'", typ)
	}

	return value, nil
}

func NewStringFromValue(value Value) (string, error) {
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
		m := value.(Monetary)
		return fmt.Sprintf("%s %s", m.Asset, m.Amount), nil
	case TypePortion:
		return value.(Portion).String(), nil
	default:
		return "", fmt.Errorf("invalid type '%v'", value.GetType())
	}
}
