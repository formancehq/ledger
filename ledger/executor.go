package ledger

import (
	"errors"
	"fmt"

	"github.com/numary/ledger/core"
	machine "github.com/numary/machine/core"
	"github.com/numary/machine/script/compiler"
	"github.com/numary/machine/vm"
)

func (l *Ledger) Execute(script core.Script) error {
	if script.Plain == "" {
		return errors.New("no script to execute")
	}

	p, err := compiler.Compile(script.Plain)
	if err != nil {
		return fmt.Errorf("compile error: %v", err)
	}
	m := vm.NewMachine(p)

	err = m.SetVarsFromJSON(script.Vars)
	if err != nil {
		return fmt.Errorf("could not set variables: %v", err)
	}

	{
		ch, err := m.ResolveResources()
		if err != nil {
			return fmt.Errorf("could not resolve program resources: %v", err)
		}
		for req := range ch {
			if req.Error != nil {
				return fmt.Errorf("could not resolve program resources: %v", req.Error)
			}
			account, err := l.GetAccount(req.Account)
			if err != nil {
				return fmt.Errorf("could not get account %q: %v", req.Account, err)
			}
			meta := account.Metadata
			entry, ok := meta[req.Key]
			if !ok {
				return fmt.Errorf("missing key %v in metadata for account %v", req.Key, req.Account)
			}
			typ := machine.TypenameToType(entry.Type)
			value, err := machine.NewValueFromJSON(typ, entry.Value)
			if err != nil {
				return fmt.Errorf("json was invalid in key %v in metadata for account %v: %v", req.Key, req.Account, err)
			}
			req.Response <- *value
		}
	}

	{
		ch, err := m.ResolveBalances()
		if err != nil {
			return fmt.Errorf("could not resolve balances: %v", err)
		}
		for req := range ch {
			if req.Error != nil {
				return fmt.Errorf("could not resolve balances: %v", err)
			}
			account, err := l.GetAccount(req.Account)
			if err != nil {
				return fmt.Errorf("could not get account %q: %v", req.Account, err)
			}
			amt := account.Balances[req.Asset]
			if amt < 0 {
				amt = 0
			}
			req.Response <- uint64(amt)
		}
	}

	c, err := m.Execute()
	if err != nil {
		return fmt.Errorf("script failed: %v", err)
	}
	if c == vm.EXIT_FAIL {
		return errors.New("script exited with error code EXIT_FAIL")
	}

	t := core.Transaction{
		Postings: m.Postings,
	}

	err = l.Commit([]core.Transaction{t})
	return err
}
