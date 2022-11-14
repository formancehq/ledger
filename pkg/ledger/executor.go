package ledger

import (
	"context"
	"encoding/json"
	"fmt"

	machine "github.com/formancehq/machine/core"
	"github.com/formancehq/machine/script/compiler"
	"github.com/formancehq/machine/vm"
	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

func (l *Ledger) ProcessScript(ctx context.Context, addOps *core.AdditionalOperations, script core.Script) (*core.TransactionData, *core.AdditionalOperations, error) {
	if script.Plain == "" {
		return nil, nil, NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	p, err := compiler.Compile(script.Plain)
	if err != nil {
		return nil, nil, NewScriptError(ScriptErrorCompilationFailed, err.Error())
	}

	m := vm.NewMachine(*p)

	if err = m.SetVarsFromJSON(script.Vars); err != nil {
		return nil, nil, NewScriptError(ScriptErrorCompilationFailed,
			fmt.Sprintf("could not set variables: %v", err))
	}

	{
		ch, err := m.ResolveResources()
		if err != nil {
			return nil, nil, fmt.Errorf("could not resolve program resources: %v", err)
		}
		for req := range ch {
			if req.Error != nil {
				return nil, nil, NewScriptError(ScriptErrorCompilationFailed,
					fmt.Sprintf("could not resolve program resources: %v", req.Error))
			}
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return nil, nil, fmt.Errorf("could not get account %q: %v", req.Account, err)
			}
			meta := account.Metadata
			entry, ok := meta[req.Key]
			if !ok {
				return nil, nil, NewScriptError(ScriptErrorCompilationFailed,
					fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
			}
			data, err := json.Marshal(entry)
			if err != nil {
				return nil, nil, errors.Wrap(err, "json.Marshal")
			}
			value, err := machine.NewValueFromTypedJSON(data)
			if err != nil {
				return nil, nil, NewScriptError(ScriptErrorCompilationFailed,
					fmt.Sprintf("invalid format for metadata at key %v for account %v: %v", req.Key, req.Account, err))
			}
			req.Response <- *value
		}
	}

	{
		ch, err := m.ResolveBalances()
		if err != nil {
			return nil, nil, fmt.Errorf("could not resolve balances: %v", err)
		}
		for req := range ch {
			if req.Error != nil {
				return nil, nil, fmt.Errorf("could not resolve balances: %v", err)
			}
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return nil, nil, fmt.Errorf("could not get account %q: %v", req.Account, err)
			}
			amt := account.Balances[req.Asset].OrZero()
			resp := machine.MonetaryInt(*amt)
			req.Response <- &resp
		}
	}

	exitCode, err := m.Execute()
	if err != nil {
		return nil, nil, fmt.Errorf("script execution failed: %v", err)
	}

	if exitCode != vm.EXIT_OK {
		switch exitCode {
		case vm.EXIT_FAIL:
			return nil, nil, errors.New("script exited with error code EXIT_FAIL")
		case vm.EXIT_FAIL_INVALID:
			return nil, nil, errors.New("internal error: compiled script was invalid")
		case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
			// TODO: If the machine can provide the asset which is failing
			// we should be able to use InsufficientFundError{} instead of error code
			return nil, nil, NewScriptError(ScriptErrorInsufficientFund,
				"account had insufficient funds")
		default:
			return nil, nil, errors.New("script execution failed")
		}
	}

	txMeta := m.GetTxMetaJSON()
	for k, v := range txMeta {
		asMapAny := make(map[string]any)
		if err := json.Unmarshal(v.([]byte), &asMapAny); err != nil {
			panic(err)
		}
		txMeta[k] = asMapAny
	}
	for k, v := range script.Metadata {
		_, ok := txMeta[k]
		if ok {
			return nil, nil, NewScriptError(ScriptErrorMetadataOverride,
				"cannot override metadata from script")
		}
		txMeta[k] = v
	}

	accMeta := core.Metadata{}
	for account, meta := range m.GetAccountsMetaJSON() {
		meta := meta.(map[string][]byte)
		for k, v := range meta {
			asMapAny := make(map[string]any)
			if err := json.Unmarshal(v, &asMapAny); err != nil {
				return nil, nil, errors.Wrap(err, "json.Unmarshal")
			}
			if _, ok := accMeta["set_account_meta"]; !ok {
				accMeta["set_account_meta"] = map[string]any{}
			}
			if account[0] == '@' {
				account = account[1:]
			}
			if _, ok := accMeta["set_account_meta"].(map[string]any)[account]; !ok {
				accMeta["set_account_meta"].(map[string]any)[account] = map[string]any{}
			}
			accMeta["set_account_meta"].(map[string]any)[account].(map[string]any)[k] = asMapAny

			if addOps == nil {
				addOps = &core.AdditionalOperations{SetAccountMeta: core.AccountsMeta{}}
			}
			if _, ok := addOps.SetAccountMeta[account]; !ok {
				addOps.SetAccountMeta[account] = map[string]any{}
			}
			addOps.SetAccountMeta[account][k] = asMapAny
		}
	}

	postings := make([]core.Posting, len(m.Postings))
	for i, p := range m.Postings {
		amt := core.MonetaryInt(*p.Amount)
		postings[i] = core.Posting{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      &amt,
			Asset:       p.Asset,
		}
	}

	return &core.TransactionData{
		Postings:  postings,
		Metadata:  core.Metadata(txMeta).Merge(accMeta),
		Reference: script.Reference,
	}, addOps, nil
}

func (l *Ledger) Execute(ctx context.Context, ops *core.AdditionalOperations, script core.Script) (*CommitResult, error) {
	txData, ops, err := l.ProcessScript(ctx, ops, script)
	if err != nil {
		return nil, err
	}

	return l.Commit(ctx, ops, *txData)
}

func (l *Ledger) ExecutePreview(ctx context.Context, ops *core.AdditionalOperations, script core.Script) (*CommitResult, error) {
	txData, ops, err := l.ProcessScript(ctx, ops, script)
	if err != nil {
		return nil, err
	}

	return l.CommitPreview(ctx, ops, *txData)
}
