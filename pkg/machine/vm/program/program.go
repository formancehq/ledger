package program

import (
	"encoding/json"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
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
	Typ    core.Type
	Name   string
	Origin VarOrigin
}

type NeededBalance struct {
	Account       Expr
	AssetOrAmount Expr
}

type Program struct {
	VarsDecl       []VarDecl
	Instruction    []Instruction
	NeededBalances map[NeededBalance]struct{}
}

func (p *Program) String() string {
	cfg := spew.NewDefaultConfig()
	cfg.Indent = "    "
	cfg.DisablePointerAddresses = true
	cfg.DisableMethods = true
	cfg.DisableCapacities = true
	return cfg.Sdump(p)
}

func (p *Program) ParseVariables(vars map[string]core.Value) (map[string]core.Value, error) {
	variables := make(map[string]core.Value)
	for _, varDecl := range p.VarsDecl {
		if varDecl.Origin == nil {
			if val, ok := vars[varDecl.Name]; ok && val.GetType() == varDecl.Typ {
				variables[varDecl.Name] = val
				switch val.GetType() {
				case core.TypeAccount:
					if err := core.ParseAccountAddress(val.(core.AccountAddress)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							varDecl.Name, string(val.(core.AccountAddress)))
					}
				case core.TypeAsset:
					if err := core.ParseAsset(val.(core.Asset)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							varDecl.Name, string(val.(core.Asset)))
					}
				case core.TypeMonetary:
					if err := core.ParseMonetary(val.(core.Monetary)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							varDecl.Name, val.(core.Monetary).String())
					}
				case core.TypePortion:
					if err := core.ValidatePortionSpecific(val.(core.Portion)); err != nil {
						return nil, errors.Wrapf(err, "invalid variable $%s value '%s'",
							varDecl.Name, val.(core.Portion).String())
					}
				case core.TypeString:
				case core.TypeNumber:
				default:
					return nil, fmt.Errorf("unsupported type for variable $%s: %s",
						varDecl.Name, val.GetType())
				}
				delete(vars, varDecl.Name)
			} else if val, ok := vars[varDecl.Name]; ok && val.GetType() != varDecl.Typ {
				return nil, fmt.Errorf("wrong type for variable $%s: %s instead of %s",
					varDecl.Name, varDecl.Typ, val.GetType())
			} else {
				return nil, fmt.Errorf("missing variable $%s", varDecl.Name)
			}
		}
	}
	for name := range vars {
		return nil, fmt.Errorf("extraneous variable $%s", name)
	}
	return variables, nil
}

func (p *Program) ParseVariablesJSON(vars map[string]json.RawMessage) (map[string]core.Value, error) {
	variables := make(map[string]core.Value)
	for _, varDecl := range p.VarsDecl {
		if varDecl.Origin == nil {
			data, ok := vars[varDecl.Name]
			if !ok {
				return nil, fmt.Errorf("missing variable $%s", varDecl.Name)
			}
			val, err := core.NewValueFromJSON(varDecl.Typ, data)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid JSON value for variable $%s of type %v: %w",
					varDecl.Name, varDecl.Typ, err)
			}
			variables[varDecl.Name] = *val
			delete(vars, varDecl.Name)
		}
	}
	for name := range vars {
		return nil, fmt.Errorf("extraneous variable $%s", name)
	}
	return variables, nil
}
