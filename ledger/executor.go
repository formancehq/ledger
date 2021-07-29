package ledger

import (
	"errors"
	"fmt"

	"github.com/numary/ledger/core"
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
		return fmt.Errorf("error while setting variables: %v", err)
	}

	needed_balances, err := m.GetNeededBalances()
	if err != nil {
		return err
	}

	balances := map[string]map[string]uint64{}

	for account_address, needed_assets := range needed_balances {
		account, err := l.GetAccount(account_address)
		if err != nil {
			return fmt.Errorf("invalid account address: %v\n", err)
		}
		balances[account_address] = map[string]uint64{}
		for asset := range needed_assets {
			amt := account.Balances[asset]
			if amt < 0 {
				amt = 0
			}
			balances[account_address][asset] = uint64(amt)
		}
	}

	err = m.SetBalances(balances)
	if err != nil {
		return err
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
