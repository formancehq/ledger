package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	machine "github.com/formancehq/machine/core"
	"github.com/formancehq/machine/script/compiler"
	"github.com/formancehq/machine/vm"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

type CommitResult struct {
	PreCommitVolumes      core.AccountsAssetsVolumes
	PostCommitVolumes     core.AccountsAssetsVolumes
	GeneratedTransactions []core.ExpandedTransaction
	AdditionalOperations  *core.AdditionalOperations
}

func (l *Ledger) Execute(ctx context.Context, checkMapping, preview bool, scripts ...core.ScriptData) (CommitResult, error) {
	if len(scripts) == 0 {
		return CommitResult{},
			NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	addOps := new(core.AdditionalOperations)

	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return CommitResult{}, errors.Wrap(err,
			"could not get last transaction")
	}

	vAggr := NewVolumeAggregator(l.store)
	txs := make([]core.ExpandedTransaction, 0)
	var nextTxId uint64
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
	}
	contracts := make([]core.Contract, 0)
	if checkMapping {
		mapping, err := l.store.LoadMapping(ctx)
		if err != nil {
			return CommitResult{}, errors.Wrap(err,
				"loading mapping")
		}
		if mapping != nil {
			contracts = append(contracts, mapping.Contracts...)
		}
		contracts = append(contracts, DefaultContracts...)
	}

	usedReferences := make(map[string]struct{})
	accountsVolumes := core.AccountsAssetsVolumes{}
	for i, script := range scripts {
		// Until v1.5.0, dates was stored as string using rfc3339 format
		// So round the date to the second to keep the same behaviour
		if script.Timestamp.IsZero() {
			script.Timestamp = time.Now().UTC().Truncate(time.Second)
		} else {
			script.Timestamp = script.Timestamp.UTC()
		}

		past := false
		if lastTx != nil && script.Timestamp.Before(lastTx.Timestamp) {
			past = true
		}
		if past && !l.allowPastTimestamps {
			return CommitResult{}, NewValidationError(fmt.Sprintf(
				"cannot pass a timestamp prior to the last transaction: %s (passed) is %s before %s (last)",
				script.Timestamp.Format(time.RFC3339Nano),
				lastTx.Timestamp.Sub(script.Timestamp),
				lastTx.Timestamp.Format(time.RFC3339Nano)))
		}

		if script.Reference != "" {
			if _, ok := usedReferences[script.Reference]; ok {
				return CommitResult{}, NewConflictError()
			}
			usedReferences[script.Reference] = struct{}{}

			txs, err := l.GetTransactions(ctx, *NewTransactionsQuery().
				WithReferenceFilter(script.Reference))
			if err != nil {
				return CommitResult{}, errors.Wrap(err, "GetTransactions")
			}
			if len(txs.Data) > 0 {
				return CommitResult{}, NewConflictError()
			}
		}

		if script.Plain == "" {
			return CommitResult{}, NewScriptError(ScriptErrorNoScript,
				"no script to execute")
		}

		p, err := compiler.Compile(script.Plain)
		if err != nil {
			return CommitResult{}, NewScriptError(ScriptErrorCompilationFailed,
				err.Error())
		}

		m := vm.NewMachine(*p)

		if err = m.SetVarsFromJSON(script.Vars); err != nil {
			return CommitResult{}, NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(err, "could not set variables").Error())
		}

		resourcesChan, err := m.ResolveResources()
		if err != nil {
			return CommitResult{}, errors.Wrap(err,
				"could not resolve program resources")
		}
		for req := range resourcesChan {
			if req.Error != nil {
				return CommitResult{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program resources").Error())
			}
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return CommitResult{}, errors.Wrap(err,
					fmt.Sprintf("could not get account %q", req.Account))
			}
			if req.Key != "" {
				entry, ok := account.Metadata[req.Key]
				if !ok {
					return CommitResult{}, NewScriptError(ScriptErrorCompilationFailed,
						fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
				}
				data, err := json.Marshal(entry)
				if err != nil {
					return CommitResult{}, errors.Wrap(err, "json.Marshal")
				}
				value, err := machine.NewValueFromTypedJSON(data)
				if err != nil {
					return CommitResult{}, NewScriptError(ScriptErrorCompilationFailed,
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
				return CommitResult{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(err, fmt.Sprintf("invalid ResourceRequest: %+v", req)).Error())
			}
		}

		balanceCh, err := m.ResolveBalances()
		if err != nil {
			return CommitResult{}, errors.Wrap(err,
				"could not resolve balances")
		}
		for req := range balanceCh {
			if req.Error != nil {
				return CommitResult{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program balances").Error())
			}
			var amt *core.MonetaryInt
			if vol, ok := accountsVolumes[req.Account]; !ok {
				account, err := l.GetAccount(ctx, req.Account)
				if err != nil {
					return CommitResult{}, errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
				}
				accountsVolumes[req.Account] = account.Volumes
				amt = account.Balances[req.Asset].OrZero()
			} else {
				amt = vol[req.Asset].Balance()
			}
			resp := machine.MonetaryInt(*amt)
			req.Response <- &resp
		}

		exitCode, err := m.Execute()
		if err != nil {
			return CommitResult{}, errors.Wrap(err,
				"script execution failed")
		}

		if exitCode != vm.EXIT_OK {
			switch exitCode {
			case vm.EXIT_FAIL:
				return CommitResult{}, errors.New(
					"script exited with error code EXIT_FAIL")
			case vm.EXIT_FAIL_INVALID:
				return CommitResult{}, errors.New(
					"internal error: compiled script was invalid")
			case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
				// TODO: If the machine can provide the asset which is failing
				// we should be able to use InsufficientFundError{} instead of error code
				return CommitResult{}, NewScriptError(ScriptErrorInsufficientFund,
					"account had insufficient funds")
			default:
				return CommitResult{}, errors.New(
					"script execution failed")
			}
		}

		if len(m.Postings) == 0 {
			return CommitResult{},
				NewValidationError("transaction has no postings")
		}

		txVolumeAggr := vAggr.NextTx()
		postings := make([]core.Posting, len(m.Postings))
		for j, posting := range m.Postings {
			amt := core.MonetaryInt(*posting.Amount)
			if err := txVolumeAggr.Transfer(ctx,
				posting.Source, posting.Destination, posting.Asset, &amt); err != nil {
				return CommitResult{}, NewTransactionCommitError(i, err)
			}
			postings[j] = core.Posting{
				Source:      posting.Source,
				Destination: posting.Destination,
				Amount:      &amt,
				Asset:       posting.Asset,
			}
		}

		accounts := make(map[string]*core.Account, 0)
		for addr, volumes := range txVolumeAggr.PostCommitVolumes() {
			accountsVolumes[addr] = volumes
			for asset, volume := range volumes {
				if addr == "world" {
					continue
				}

				for _, contract := range contracts {
					if contract.Match(addr) {
						if _, ok := accounts[addr]; !ok {
							account, err := l.store.GetAccount(ctx, addr)
							if err != nil {
								return CommitResult{}, NewTransactionCommitError(i,
									errors.Wrap(err, fmt.Sprintf("GetAccount '%s'", addr)))
							}
							accounts[addr] = account
						}
						if ok := contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": volume.Balance(),
							},
							Metadata: accounts[addr].Metadata,
							Asset:    asset,
						}); !ok {
							return CommitResult{}, NewInsufficientFundError(asset)
						}
						break
					}
				}
			}
		}

		metadata := m.GetTxMetaJSON()
		for k, v := range metadata {
			asMapAny := make(map[string]any)
			if err := json.Unmarshal(v.([]byte), &asMapAny); err != nil {
				return CommitResult{}, errors.Wrap(err, "json.Unmarshal")
			}
			metadata[k] = asMapAny
		}
		for k, v := range script.Metadata {
			_, ok := metadata[k]
			if ok {
				return CommitResult{}, NewScriptError(ScriptErrorMetadataOverride,
					"cannot override metadata from script")
			}
			metadata[k] = v
		}

		for account, meta := range m.GetAccountsMetaJSON() {
			meta := meta.(map[string][]byte)
			for k, v := range meta {
				asMapAny := make(map[string]any)
				if err := json.Unmarshal(v, &asMapAny); err != nil {
					return CommitResult{}, errors.Wrap(err, "json.Unmarshal")
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

		tx := core.ExpandedTransaction{
			Transaction: core.Transaction{
				TransactionData: core.TransactionData{
					Postings:  postings,
					Reference: script.Reference,
					Metadata:  core.Metadata(metadata),
					Timestamp: script.Timestamp,
				},
				ID: nextTxId,
			},
			PostCommitVolumes: txVolumeAggr.PostCommitVolumes(),
			PreCommitVolumes:  txVolumeAggr.PreCommitVolumes(),
		}
		lastTx = &tx
		txs = append(txs, tx)
		nextTxId++
	}

	res := CommitResult{
		PreCommitVolumes:      vAggr.AggregatedPreCommitVolumes(),
		PostCommitVolumes:     vAggr.AggregatedPostCommitVolumes(),
		GeneratedTransactions: txs,
		AdditionalOperations:  addOps,
	}

	if preview {
		return res, nil
	}

	if err := l.store.Commit(ctx, txs...); err != nil {
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			return CommitResult{}, NewConflictError()
		default:
			return CommitResult{}, errors.Wrap(err,
				"committing transactions")
		}
	}

	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			if err := l.store.UpdateAccountMetadata(ctx,
				addr, m, time.Now().Round(time.Second).UTC()); err != nil {
				return CommitResult{}, errors.Wrap(err,
					"updating account metadata")
			}
		}
	}

	l.monitor.CommittedTransactions(ctx, l.store.Name(), res)
	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			l.monitor.SavedMetadata(ctx,
				l.store.Name(), core.MetaTargetTypeAccount, addr, m)
		}
	}

	return res, nil
}
