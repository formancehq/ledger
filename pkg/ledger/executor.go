package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/numary/ledger/pkg/core"
	machine "github.com/numary/machine/core"
	"github.com/numary/machine/script/compiler"
	"github.com/numary/machine/vm"
)

func (l *Ledger) execute(ctx context.Context, script core.Script) (*core.TransactionData, error) {
	if script.Plain == "" {
		return nil, NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	p, err := compiler.Compile(script.Plain)
	if err != nil {
		return nil, NewScriptError(ScriptErrorCompilationFailed, err.Error())
	}

	m := vm.NewMachine(*p)

	err = m.SetVarsFromJSON(script.Vars)
	if err != nil {
		return nil, NewScriptError(ScriptErrorCompilationFailed, fmt.Sprintf("could not set variables: %v", err))
	}

	{
		ch, err := m.ResolveResources()
		if err != nil {
			return nil, fmt.Errorf("could not resolve program resources: %v", err)
		}
		for req := range ch {
			if req.Error != nil {
				return nil, NewScriptError(ScriptErrorCompilationFailed, fmt.Sprintf("could not resolve program resources: %v", req.Error))
			}
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return nil, fmt.Errorf("could not get account %q: %v", req.Account, err)
			}
			meta := account.Metadata
			entry, ok := meta[req.Key]
			if !ok {
				return nil, NewScriptError(ScriptErrorCompilationFailed, fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
			}
			data, err := json.Marshal(entry)
			if err != nil {
				return nil, err
			}
			value, err := machine.NewValueFromTypedJSON(data)
			if err != nil {
				return nil, NewScriptError(ScriptErrorCompilationFailed, fmt.Sprintf("invalid format for metadata at key %v for account %v: %v", req.Key, req.Account, err))
			}
			req.Response <- *value
		}
	}

	{
		ch, err := m.ResolveBalances()
		if err != nil {
			return nil, fmt.Errorf("could not resolve balances: %v", err)
		}
		for req := range ch {
			if req.Error != nil {
				return nil, fmt.Errorf("could not resolve balances: %v", err)
			}
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return nil, fmt.Errorf("could not get account %q: %v", req.Account, err)
			}
			amt := account.Balances[req.Asset]
			req.Response <- *amt.OrZero()
		}
	}

	exitCode, err := m.Execute()
	if err != nil {
		return nil, fmt.Errorf("script execution failed: %v", err)
	}

	if exitCode != vm.EXIT_OK {
		switch exitCode {
		case vm.EXIT_FAIL:
			return nil, errors.New("script exited with error code EXIT_FAIL")
		case vm.EXIT_FAIL_INVALID:
			return nil, errors.New("internal error: compiled script was invalid")
		case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
			// TODO: If the machine can provide the asset which is failing, we should be able to use InsufficientFundError{} instead of error code
			return nil, NewScriptError(ScriptErrorInsufficientFund, "account had insufficient funds")
		default:
			return nil, errors.New("script execution failed")
		}
	}

	metadata := m.GetTxMetaJson()
	for k, v := range metadata {
		asMapAny := make(map[string]any)
		err := json.Unmarshal(v.([]byte), &asMapAny)
		if err != nil {
			panic(err)
		}
		metadata[k] = asMapAny
	}
	for k, v := range script.Metadata {
		_, ok := metadata[k]
		if ok {
			return nil, NewScriptError(ScriptErrorMetadataOverride, "cannot override metadata from script")
		}
		metadata[k] = v
	}

	t := &core.TransactionData{
		Postings:  m.Postings,
		Metadata:  metadata,
		Reference: script.Reference,
	}

	return t, nil
}

func (l *Ledger) Execute(ctx context.Context, script core.Script) (*core.ExpandedTransaction, error) {
	t, err := l.execute(ctx, script)
	if err != nil {
		return nil, err
	}

	res, err := l.Commit(ctx, []core.TransactionData{*t})
	if err != nil {
		return nil, err
	}

	return &res[0], nil
}

func (l *Ledger) ExecutePreview(ctx context.Context, script core.Script) (*core.ExpandedTransaction, error) {
	t, err := l.execute(ctx, script)
	if err != nil {
		return nil, err
	}

	res, err := l.CommitPreview(ctx, []core.TransactionData{*t})
	if err != nil {
		return nil, err
	}

	return &res[0], nil
}
