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

func (l *Ledger) Execute(ctx context.Context, preview bool, scripts ...core.ScriptData) ([]core.ExpandedTransaction, error) {
	if len(scripts) == 0 {
		return []core.ExpandedTransaction{},
			NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	txsData, addOps, err := l.ProcessScripts(ctx, scripts...)
	if err != nil {
		return []core.ExpandedTransaction{}, err
	}

	for _, txData := range txsData {
		if len(txData.Postings) == 0 {
			return []core.ExpandedTransaction{},
				NewValidationError("transaction has no postings")
		}
	}

	txs, err := l.Commit(ctx, preview, addOps, txsData...)
	if err != nil {
		return []core.ExpandedTransaction{}, err
	}

	return txs, nil
}

func (l *Ledger) ProcessScripts(ctx context.Context, scripts ...core.ScriptData) ([]core.TransactionData, *core.AdditionalOperations, error) {
	txsData := []core.TransactionData{}
	addOps := new(core.AdditionalOperations)

	for _, script := range scripts {
		if script.Reference != "" {
			txs, err := l.GetTransactions(ctx, *NewTransactionsQuery().
				WithReferenceFilter(script.Reference))
			if err != nil {
				return []core.TransactionData{}, nil, errors.Wrap(err, "GetTransactions")
			}
			if len(txs.Data) > 0 {
				return []core.TransactionData{}, nil, NewConflictError()
			}
		}

		if script.Plain == "" {
			return []core.TransactionData{}, nil, NewScriptError(ScriptErrorNoScript,
				"no script to execute")
		}

		p, err := compiler.Compile(script.Plain)
		if err != nil {
			return []core.TransactionData{}, nil, NewScriptError(ScriptErrorCompilationFailed,
				err.Error())
		}

		m := vm.NewMachine(*p)

		if err = m.SetVarsFromJSON(script.Vars); err != nil {
			return []core.TransactionData{}, nil, NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(err, "could not set variables").Error())
		}

		{
			ch, err := m.ResolveResources()
			if err != nil {
				return []core.TransactionData{}, nil, errors.Wrap(err,
					"could not resolve program resources")
			}
			for req := range ch {
				if req.Error != nil {
					return []core.TransactionData{}, nil, NewScriptError(ScriptErrorCompilationFailed,
						errors.Wrap(req.Error, "could not resolve program resources").Error())
				}
				account, err := l.GetAccount(ctx, req.Account)
				if err != nil {
					return []core.TransactionData{}, nil, errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
				}
				if req.Key != "" {
					entry, ok := account.Metadata[req.Key]
					if !ok {
						return []core.TransactionData{}, nil, NewScriptError(ScriptErrorCompilationFailed,
							fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
					}
					data, err := json.Marshal(entry)
					if err != nil {
						return []core.TransactionData{}, nil, errors.Wrap(err, "json.Marshal")
					}
					value, err := machine.NewValueFromTypedJSON(data)
					if err != nil {
						return []core.TransactionData{}, nil, NewScriptError(ScriptErrorCompilationFailed,
							errors.Wrap(err, fmt.Sprintf(
								"invalid format for metadata at key %v for account %v",
								req.Key, req.Account)).Error())
					}
					req.Response <- *value
				} else if req.Asset != "" {
					amt := account.Balances[req.Asset].OrZero()
					resp := machine.MonetaryInt(*amt)
					req.Response <- &resp
				} else {
					return []core.TransactionData{}, nil, NewScriptError(ScriptErrorCompilationFailed,
						errors.Wrap(err, fmt.Sprintf("invalid ResourceRequest: %+v", req)).Error())
				}
			}
		}

		{
			ch, err := m.ResolveBalances()
			if err != nil {
				return []core.TransactionData{}, nil, errors.Wrap(err,
					"could not resolve balances")
			}
			for req := range ch {
				if req.Error != nil {
					return []core.TransactionData{}, nil, NewScriptError(ScriptErrorCompilationFailed,
						errors.Wrap(req.Error, "could not resolve program balances").Error())
				}
				account, err := l.GetAccount(ctx, req.Account)
				if err != nil {
					return []core.TransactionData{}, nil, errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
				}
				amt := account.Balances[req.Asset].OrZero()
				resp := machine.MonetaryInt(*amt)
				req.Response <- &resp
			}
		}

		exitCode, err := m.Execute()
		if err != nil {
			return []core.TransactionData{}, nil, errors.Wrap(err, "script execution failed")
		}

		if exitCode != vm.EXIT_OK {
			switch exitCode {
			case vm.EXIT_FAIL:
				return []core.TransactionData{}, nil, errors.New("script exited with error code EXIT_FAIL")
			case vm.EXIT_FAIL_INVALID:
				return []core.TransactionData{}, nil, errors.New("internal error: compiled script was invalid")
			case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
				// TODO: If the machine can provide the asset which is failing
				// we should be able to use InsufficientFundError{} instead of error code
				return []core.TransactionData{}, nil, NewScriptError(ScriptErrorInsufficientFund,
					"account had insufficient funds")
			default:
				return []core.TransactionData{}, nil, errors.New("script execution failed")
			}
		}

		metadata := m.GetTxMetaJSON()
		for k, v := range metadata {
			asMapAny := make(map[string]any)
			if err := json.Unmarshal(v.([]byte), &asMapAny); err != nil {
				return []core.TransactionData{}, nil, errors.Wrap(err, "json.Unmarshal")
			}
			metadata[k] = asMapAny
		}
		for k, v := range script.Metadata {
			_, ok := metadata[k]
			if ok {
				return []core.TransactionData{}, nil, NewScriptError(ScriptErrorMetadataOverride,
					"cannot override metadata from script")
			}
			metadata[k] = v
		}

		for account, meta := range m.GetAccountsMetaJSON() {
			meta := meta.(map[string][]byte)
			for k, v := range meta {
				asMapAny := make(map[string]any)
				if err := json.Unmarshal(v, &asMapAny); err != nil {
					return []core.TransactionData{}, nil, errors.Wrap(err, "json.Unmarshal")
				}
				if account[0] == '@' {
					account = account[1:]
				}
				if addOps.SetAccountMeta == nil {
					addOps.SetAccountMeta = core.AccountsMeta{}
				}
				if _, ok := addOps.SetAccountMeta[account]; !ok {
					addOps.SetAccountMeta[account] = core.Metadata{}
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

		txsData = append(txsData, core.TransactionData{
			Postings:  postings,
			Reference: script.Reference,
			Metadata:  core.Metadata(metadata),
			Timestamp: script.Timestamp,
		})
	}

	return txsData, addOps, nil
}
