package ledger

import (
	"errors"

	"github.com/numary/ledger/core"
	"github.com/numary/machine/script/compiler"
	"github.com/numary/machine/vm"
)

func (l *Ledger) Execute(script core.Script) error {
	if script.Plain == "" {
		return errors.New("no script to execute")
	}

	p, err := compiler.Compile(script.Plain)
	m := vm.NewMachine(p)

	if err != nil {
		return err
	}

	if c, err := m.ExecuteFromJSON(script.Vars); err != nil || c == vm.EXIT_FAIL {
		return errors.New("script failed")
	}

	t := core.Transaction{
		Postings: m.Postings,
	}

	err = l.Commit([]core.Transaction{t})
	return err
}
