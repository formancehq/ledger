package program

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/pkg/machine/internal"
	"github.com/pkg/errors"
)

type Program struct {
	Instructions   []byte
	Resources      []Resource
	Sources        []internal.Address
	NeededBalances map[internal.Address]map[internal.Address]struct{}
}

func (p Program) String() string {
	out := "Program:\nINSTRUCTIONS\n"
	for i := 0; i < len(p.Instructions); i++ {
		out += fmt.Sprintf("%02d----- ", i)
		switch p.Instructions[i] {
		case OP_APUSH:
			out += "OP_APUSH "
			address := binary.LittleEndian.Uint16(p.Instructions[i+1 : i+3])
			out += fmt.Sprintf("#%d\n", address)
			i += 2
		default:
			out += OpcodeName(p.Instructions[i]) + "\n"
		}
	}

	out += fmt.Sprintln("RESOURCES")
	i := 0
	for i = 0; i < len(p.Resources); i++ {
		out += fmt.Sprintf("%02d ", i)
		out += fmt.Sprintf("%v\n", p.Resources[i])
	}
	return out
}

func (p *Program) ParseVariables(vars map[string]internal.Value) (map[string]internal.Value, error) {
	variables := make(map[string]internal.Value)
	for _, res := range p.Resources {
		if variable, ok := res.(Variable); ok {
			if val, ok := vars[variable.Name]; ok && val.GetType() == variable.Typ {
				variables[variable.Name] = val
				switch val.GetType() {
				case internal.TypeAccount:
					if err := internal.ValidateAccountAddress(val.(internal.AccountAddress)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, string(val.(internal.AccountAddress)))
					}
				case internal.TypeAsset:
					if err := internal.ValidateAsset(val.(internal.Asset)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, string(val.(internal.Asset)))
					}
				case internal.TypeMonetary:
					if err := internal.ParseMonetary(val.(internal.Monetary)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, val.(internal.Monetary).String())
					}
				case internal.TypePortion:
					if err := internal.ValidatePortionSpecific(val.(internal.Portion)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, val.(internal.Portion).String())
					}
				case internal.TypeString:
				case internal.TypeNumber:
				default:
					return nil, fmt.Errorf("unsupported type for variable $%s: %s",
						variable.Name, val.GetType())
				}
				delete(vars, variable.Name)
			} else if val, ok := vars[variable.Name]; ok && val.GetType() != variable.Typ {
				return nil, fmt.Errorf("wrong type for variable $%s: %s instead of %s",
					variable.Name, variable.Typ, val.GetType())
			} else {
				return nil, fmt.Errorf("missing variable $%s", variable.Name)
			}
		}
	}
	for name := range vars {
		return nil, fmt.Errorf("extraneous variable $%s", name)
	}
	return variables, nil
}

func (p *Program) ParseVariablesJSON(vars map[string]string) (map[string]internal.Value, error) {
	variables := make(map[string]internal.Value)
	for _, res := range p.Resources {
		if param, ok := res.(Variable); ok {
			data, ok := vars[param.Name]
			if !ok {
				return nil, fmt.Errorf("missing variable $%s", param.Name)
			}
			val, err := internal.NewValueFromString(param.Typ, data)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid JSON value for variable $%s of type %v: %w",
					param.Name, param.Typ, err)
			}
			variables[param.Name] = val
			delete(vars, param.Name)
		}
	}
	for name := range vars {
		return nil, fmt.Errorf("extraneous variable $%s", name)
	}
	return variables, nil
}
