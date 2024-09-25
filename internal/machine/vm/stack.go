package vm

import (
	"fmt"

	"github.com/formancehq/ledger/v2/internal/machine"
)

func (m *Machine) popValue() machine.Value {
	l := len(m.Stack)
	x := m.Stack[l-1]
	m.Stack = m.Stack[:l-1]
	return x
}

func pop[T machine.Value](m *Machine) T {
	x := m.popValue()
	if v, ok := x.(T); ok {
		return v
	}
	panic(fmt.Errorf("unexpected type '%T' on stack", x))
}

func (m *Machine) pushValue(v machine.Value) {
	m.Stack = append(m.Stack, v)
}
