package program

import (
	"fmt"

	"github.com/formancehq/ledger/v2/internal/machine"
)

type Resource interface {
	GetType() machine.Type
}

type Constant struct {
	Inner machine.Value
}

func (c Constant) GetType() machine.Type { return c.Inner.GetType() }
func (c Constant) String() string        { return fmt.Sprintf("%v", c.Inner) }

type Variable struct {
	Typ  machine.Type
	Name string
}

func (p Variable) GetType() machine.Type { return p.Typ }
func (p Variable) String() string        { return fmt.Sprintf("<%v %v>", p.Typ, p.Name) }

type VariableAccountMetadata struct {
	Typ     machine.Type
	Name    string
	Account machine.Address
	Key     string
}

func (m VariableAccountMetadata) GetType() machine.Type { return m.Typ }
func (m VariableAccountMetadata) String() string {
	return fmt.Sprintf("<%v %v meta(%v, %v)>", m.Typ, m.Name, m.Account, m.Key)
}

type VariableAccountBalance struct {
	Name    string
	Account machine.Address
	Asset   machine.Address
}

func (a VariableAccountBalance) GetType() machine.Type { return machine.TypeMonetary }
func (a VariableAccountBalance) String() string {
	return fmt.Sprintf("<%v %v balance(%v, %v)>", machine.TypeMonetary, a.Name, a.Account, a.Asset)
}

type Monetary struct {
	Asset  machine.Address
	Amount *machine.MonetaryInt
}

func (a Monetary) GetType() machine.Type { return machine.TypeMonetary }
func (a Monetary) String() string {
	return fmt.Sprintf("<%v [%v %v]>", machine.TypeMonetary, a.Asset, a.Amount)
}
