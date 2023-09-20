package program

import (
	"encoding/binary"
	"fmt"

	internal2 "github.com/formancehq/ledger/internal/machine/internal"
	"github.com/pkg/errors"
)

type Program struct {
	Instructions   []byte
	Resources      []Resource
	Sources        []internal2.Address
	NeededBalances map[internal2.Address]map[internal2.Address]struct{}
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

func (p *Program) ParseVariables(vars map[string]internal2.Value) (map[string]internal2.Value, error) {
	variables := make(map[string]internal2.Value)
	for _, res := range p.Resources {
		if variable, ok := res.(Variable); ok {
			if val, ok := vars[variable.Name]; ok && val.GetType() == variable.Typ {
				variables[variable.Name] = val
				switch val.GetType() {
				case internal2.TypeAccount:
					if err := internal2.ValidateAccountAddress(val.(internal2.AccountAddress)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, string(val.(internal2.AccountAddress)))
					}
				case internal2.TypeAsset:
					if err := internal2.ValidateAsset(val.(internal2.Asset)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, string(val.(internal2.Asset)))
					}
				case internal2.TypeMonetary:
					if err := internal2.ParseMonetary(val.(internal2.Monetary)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, val.(internal2.Monetary).String())
					}
				case internal2.TypePortion:
					if err := internal2.ValidatePortionSpecific(val.(internal2.Portion)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, val.(internal2.Portion).String())
					}
				case internal2.TypeString:
				case internal2.TypeNumber:
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

func (p *Program) ParseVariablesJSON(vars map[string]string) (map[string]internal2.Value, error) {
	variables := make(map[string]internal2.Value)
	for _, res := range p.Resources {
		if param, ok := res.(Variable); ok {
			data, ok := vars[param.Name]
			if !ok {
				return nil, fmt.Errorf("missing variable $%s", param.Name)
			}
			val, err := internal2.NewValueFromString(param.Typ, data)
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
