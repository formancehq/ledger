package program

import (
	"fmt"

	"github.com/formancehq/ledger/pkg/machine/internal"
)

type Resource interface {
	GetType() internal.Type
}

type Constant struct {
	Inner internal.Value
}

func (c Constant) GetType() internal.Type { return c.Inner.GetType() }
func (c Constant) String() string         { return fmt.Sprintf("%v", c.Inner) }

type Variable struct {
	Typ  internal.Type
	Name string
}

func (p Variable) GetType() internal.Type { return p.Typ }
func (p Variable) String() string         { return fmt.Sprintf("<%v %v>", p.Typ, p.Name) }

type VariableAccountMetadata struct {
	Typ     internal.Type
	Name    string
	Account internal.Address
	Key     string
}

func (m VariableAccountMetadata) GetType() internal.Type { return m.Typ }
func (m VariableAccountMetadata) String() string {
	return fmt.Sprintf("<%v %v meta(%v, %v)>", m.Typ, m.Name, m.Account, m.Key)
}

type VariableAccountBalance struct {
	Name    string
	Account internal.Address
	Asset   internal.Address
}

func (a VariableAccountBalance) GetType() internal.Type { return internal.TypeMonetary }
func (a VariableAccountBalance) String() string {
	return fmt.Sprintf("<%v %v balance(%v, %v)>", internal.TypeMonetary, a.Name, a.Account, a.Asset)
}

type Monetary struct {
	Asset  internal.Address
	Amount *internal.MonetaryInt
}

func (a Monetary) GetType() internal.Type { return internal.TypeMonetary }
func (a Monetary) String() string {
	return fmt.Sprintf("<%v [%v %v]>", internal.TypeMonetary, a.Asset, a.Amount)
}
