package program

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/numary/ledger/pkg/machine/internal"
)

type Instruction interface {
	isInstruction()
}

type InstructionFail struct{}

func (s InstructionFail) isInstruction() {}

type InstructionPrint struct{ Expr Expr }

func (s InstructionPrint) isInstruction() {}

type InstructionSave struct {
	Amount  Expr
	Account Expr
}

func (s InstructionSave) isInstruction() {}

type InstructionSaveAll struct {
	Asset   Expr
	Account Expr
}

func (s InstructionSaveAll) isInstruction() {}

type InstructionAllocate struct {
	Funding     Expr
	Destination Destination
}

func (s InstructionAllocate) isInstruction() {}

type InstructionSetTxMeta struct {
	Key   string
	Value Expr
}

func (s InstructionSetTxMeta) isInstruction() {}

type InstructionSetAccountMeta struct {
	Account Expr
	Key     string
	Value   Expr
}

func (s InstructionSetAccountMeta) isInstruction() {}

type VarOrigin interface {
	isVarOrigin()
}

type VarOriginMeta struct {
	Account Expr
	Key     string
}

func (v VarOriginMeta) isVarOrigin() {}

type VarOriginBalance struct {
	Account Expr
	Asset   Expr
}

func (v VarOriginBalance) isVarOrigin() {}

type VarDecl struct {
	Typ    internal.Type
	Name   string
	Origin VarOrigin
}

type Program struct {
	VarsDecl    []VarDecl
	Instruction []Instruction
}

func (p *Program) String() string {
	cfg := spew.NewDefaultConfig()
	cfg.Indent = "    "
	cfg.DisablePointerAddresses = true
	cfg.DisableMethods = true
	cfg.DisableCapacities = true
	return cfg.Sdump(p)
}

// func (p *Program) ParseVariables(vars map[string]internal.Value) (map[string]internal.Value, error) {
// 	variables := make(map[string]internal.Value)
// 	for _, res := range p.Resources {
// 		if variable, ok := res.(Variable); ok {
// 			if val, ok := vars[variable.Name]; ok && val.GetType() == variable.Typ {
// 				variables[variable.Name] = val
// 				switch val.GetType() {
// 				case internal.TypeAccount:
// 					if err := internal.ParseAccountAddress(val.(internal.AccountAddress)); err != nil {
// 						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
// 							variable.Name, string(val.(internal.AccountAddress)))
// 					}
// 				case internal.TypeAsset:
// 					if err := internal.ParseAsset(val.(internal.Asset)); err != nil {
// 						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
// 							variable.Name, string(val.(internal.Asset)))
// 					}
// 				case internal.TypeMonetary:
// 					if err := internal.ParseMonetary(val.(internal.Monetary)); err != nil {
// 						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
// 							variable.Name, val.(internal.Monetary).String())
// 					}
// 				case internal.TypePortion:
// 					if err := internal.ValidatePortionSpecific(val.(internal.Portion)); err != nil {
// 						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
// 							variable.Name, val.(internal.Portion).String())
// 					}
// 				case internal.TypeString:
// 				case internal.TypeNumber:
// 				default:
// 					return nil, fmt.Errorf("unsupported type for variable $%s: %s",
// 						variable.Name, val.GetType())
// 				}
// 				delete(vars, variable.Name)
// 			} else if val, ok := vars[variable.Name]; ok && val.GetType() != variable.Typ {
// 				return nil, fmt.Errorf("wrong type for variable $%s: %s instead of %s",
// 					variable.Name, variable.Typ, val.GetType())
// 			} else {
// 				return nil, fmt.Errorf("missing variable $%s", variable.Name)
// 			}
// 		}
// 	}
// 	for name := range vars {
// 		return nil, fmt.Errorf("extraneous variable $%s", name)
// 	}
// 	return variables, nil
// }

func (p *Program) ParseVariablesJSON(vars map[string]string) (map[string]internal.Value, error) {
	variables := make(map[string]internal.Value)
	for _, varDecl := range p.VarsDecl {
		if varDecl.Origin != nil {
			continue
		}
		data, ok := vars[varDecl.Name]
		if !ok {
			return nil, fmt.Errorf("missing variable $%s", varDecl.Name)
		}
		val, err := internal.NewValueFromString(varDecl.Typ, data)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid JSON value for variable $%s of type %v: %w",
				varDecl.Name, varDecl.Typ, err)
		}
		variables[varDecl.Name] = val
		delete(vars, varDecl.Name)
	}
	for name := range vars {
		return nil, fmt.Errorf("extraneous variable $%s", name)
	}
	return variables, nil
}
