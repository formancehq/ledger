package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/numary/ledger/pkg/core"
	machine "github.com/numary/machine/core"
	"github.com/numary/machine/script/compiler"
	"github.com/numary/machine/vm"
)

func (l *Ledger) Execute(ctx context.Context, script core.Script) error {
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
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return fmt.Errorf("could not get account %q: %v", req.Account, err)
			}
			meta := account.Metadata
			entry, ok := meta[req.Key]
			if !ok {
				return fmt.Errorf("missing key %v in metadata for account %v", req.Key, req.Account)
			}
			value, err := machine.NewValueFromTypedJSON(entry)
			if err != nil {
				return fmt.Errorf("invalid format for metadata at key %v for account %v: %v", req.Key, req.Account, err)
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
			account, err := l.GetAccount(ctx, req.Account)
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

	exit_code, err := m.Execute()

	if err != nil {
		return fmt.Errorf("script execution failed: %v", err)
	}

	if exit_code != vm.EXIT_OK {
		switch exit_code {
		case vm.EXIT_FAIL:
			return errors.New("script exited with error code EXIT_FAIL")
		case vm.EXIT_FAIL_INVALID:
			return errors.New("internal error: compiled script was invalid")
		case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
			return errors.New("account had insufficient funds")
		default:
			return errors.New("script execution failed")
		}
	}

	t := core.TransactionData{
		Postings: m.Postings,
		Metadata: m.GetTxMetaJson(),
	}

	_, ret, err := l.Commit(ctx, []core.TransactionData{t})
	switch err {
	case ErrCommitError:
		return ret[0].Err
	default:
		return err
	}
}
