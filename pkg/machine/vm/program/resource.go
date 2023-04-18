package program

import (
	"fmt"

	"github.com/numary/ledger/pkg/core"
)

type Resource interface {
	GetType() core.Type
}

type Constant struct {
	Inner core.Value
}

func (c Constant) GetType() core.Type { return c.Inner.GetType() }
func (c Constant) String() string     { return fmt.Sprintf("%v", c.Inner) }

type Variable struct {
	Typ  core.Type
	Name string
}

func (p Variable) GetType() core.Type { return p.Typ }
func (p Variable) String() string     { return fmt.Sprintf("<%v %v>", p.Typ, p.Name) }

type VariableAccountMetadata struct {
	Typ     core.Type
	Name    string
	Account core.Address
	Key     string
}

func (m VariableAccountMetadata) GetType() core.Type { return m.Typ }
func (m VariableAccountMetadata) String() string {
	return fmt.Sprintf("<%v %v meta(%v, %v)>", m.Typ, m.Name, m.Account, m.Key)
}

type VariableAccountBalance struct {
	Name    string
	Account core.Address
	Asset   core.Address
}

func (a VariableAccountBalance) GetType() core.Type { return core.TypeMonetary }
func (a VariableAccountBalance) String() string {
	return fmt.Sprintf("<%v %v balance(%v, %v)>", core.TypeMonetary, a.Name, a.Account, a.Asset)
}

type Monetary struct {
	Asset  core.Address
	Amount *core.MonetaryInt
}

func (a Monetary) GetType() core.Type { return core.TypeMonetary }
func (a Monetary) String() string {
	return fmt.Sprintf("<%v [%v %v]>", core.TypeMonetary, a.Asset, a.Amount)
}
