package vm

import (
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
)

func (m *Machine) popValue() core.Value {
	l := len(m.Stack)
	x := m.Stack[l-1]
	m.Stack = m.Stack[:l-1]
	return x
}

func pop[T core.Value](m *Machine) T {
	x := m.popValue()
	if v, ok := x.(T); ok {
		return v
	}
	panic(fmt.Errorf("unexpected type '%T' on stack", x))
}

func (m *Machine) pushValue(v core.Value) {
	m.Stack = append(m.Stack, v)
}
