package vm

import (
	"fmt"

	"github.com/formancehq/ledger/internal/machine/internal"
)

func (m *Machine) popValue() internal.Value {
	l := len(m.Stack)
	x := m.Stack[l-1]
	m.Stack = m.Stack[:l-1]
	return x
}

func pop[T internal.Value](m *Machine) T {
	x := m.popValue()
	if v, ok := x.(T); ok {
		return v
	}
	panic(fmt.Errorf("unexpected type '%T' on stack", x))
}

func (m *Machine) pushValue(v internal.Value) {
	m.Stack = append(m.Stack, v)
}
