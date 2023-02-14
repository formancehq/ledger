package program

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

type Program struct {
	Instructions   []byte
	Resources      []Resource
	NeededBalances map[core.Address]map[core.Address]struct{}
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
		case OP_IPUSH:
			out += "OP_IPUSH "
			out += fmt.Sprintf("%d\n", binary.LittleEndian.Uint64(p.Instructions[i+1:i+9]))
			i += 8
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

func (p *Program) ParseVariables(vars map[string]core.Value) (map[string]core.Value, error) {
	variables := make(map[string]core.Value)
	for _, res := range p.Resources {
		if variable, ok := res.(Variable); ok {
			if val, ok := vars[variable.Name]; ok && val.GetType() == variable.Typ {
				variables[variable.Name] = val
				switch val.GetType() {
				case core.TypeAccount:
					if err := core.ParseAccountAddress(val.(core.AccountAddress)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, string(val.(core.AccountAddress)))
					}
				case core.TypeMonetary:
					if err := core.ParseMonetary(val.(core.Monetary)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, val.(core.Monetary).String())
					}
				case core.TypePortion:
					if err := core.ValidatePortionSpecific(val.(core.Portion)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							variable.Name, val.(core.Portion).String())
					}
				case core.TypeString:
					if _, ok := val.(core.String); !ok {
						return nil, fmt.Errorf("invalid variable $%s value '%s'",
							variable.Name, val)
					}
				case core.TypeNumber:
					if _, ok := val.(core.Number); !ok {
						return nil, fmt.Errorf("invalid variable $%s value '%s'",
							variable.Name, val)
					}
				}
				delete(vars, variable.Name)
			} else {
				return nil, fmt.Errorf("missing variables: %q", variable.Name)
			}
		}
	}
	for name := range vars {
		return nil, fmt.Errorf("extraneous variable: %q", name)
	}
	return variables, nil
}

func (p *Program) ParseVariablesJSON(vars map[string]json.RawMessage) (map[string]core.Value, error) {
	variables := make(map[string]core.Value)
	for _, res := range p.Resources {
		if param, ok := res.(Variable); ok {
			data, ok := vars[param.Name]
			if !ok {
				return nil, fmt.Errorf("missing variable $%s", param.Name)
			}
			val, err := core.NewValueFromJSON(param.Typ, data)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid JSON value for variable $%s of type %v: %w",
					param.Name, param.Typ, err)
			}
			variables[param.Name] = *val
			delete(vars, param.Name)
		}
	}
	for name := range vars {
		return nil, fmt.Errorf("extraneous variable $%s", name)
	}
	return variables, nil
}
