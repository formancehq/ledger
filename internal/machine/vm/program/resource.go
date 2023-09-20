package program

import (
	"fmt"

	internal2 "github.com/formancehq/ledger/internal/machine/internal"
)

type Resource interface {
	GetType() internal2.Type
}

type Constant struct {
	Inner internal2.Value
}

func (c Constant) GetType() internal2.Type { return c.Inner.GetType() }
func (c Constant) String() string          { return fmt.Sprintf("%v", c.Inner) }

type Variable struct {
	Typ  internal2.Type
	Name string
}

func (p Variable) GetType() internal2.Type { return p.Typ }
func (p Variable) String() string          { return fmt.Sprintf("<%v %v>", p.Typ, p.Name) }

type VariableAccountMetadata struct {
	Typ     internal2.Type
	Name    string
	Account internal2.Address
	Key     string
}

func (m VariableAccountMetadata) GetType() internal2.Type { return m.Typ }
func (m VariableAccountMetadata) String() string {
	return fmt.Sprintf("<%v %v meta(%v, %v)>", m.Typ, m.Name, m.Account, m.Key)
}

type VariableAccountBalance struct {
	Name    string
	Account internal2.Address
	Asset   internal2.Address
}

func (a VariableAccountBalance) GetType() internal2.Type { return internal2.TypeMonetary }
func (a VariableAccountBalance) String() string {
	return fmt.Sprintf("<%v %v balance(%v, %v)>", internal2.TypeMonetary, a.Name, a.Account, a.Asset)
}

type Monetary struct {
	Asset  internal2.Address
	Amount *internal2.MonetaryInt
}

func (a Monetary) GetType() internal2.Type { return internal2.TypeMonetary }
func (a Monetary) String() string {
	return fmt.Sprintf("<%v [%v %v]>", internal2.TypeMonetary, a.Asset, a.Amount)
}
