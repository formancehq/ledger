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
}

func (l *Ledger) ExecuteScripts(ctx context.Context, checkMapping, preview bool, scripts ...core.ScriptData) ([]core.ExpandedTransaction, error) {
	if len(scripts) == 0 {
		return []core.ExpandedTransaction{},
			NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	txsData := []core.TransactionData{}
	addOps := new(core.AdditionalOperations)

	for _, script := range scripts {
		if script.Reference != "" {
			txs, err := l.GetTransactions(ctx, *NewTransactionsQuery().
				WithReferenceFilter(script.Reference))
			if err != nil {
				return []core.ExpandedTransaction{}, errors.Wrap(err, "GetTransactions")
			}
			if len(txs.Data) > 0 {
				return []core.ExpandedTransaction{}, NewConflictError()
			}
		}

		if script.Plain == "" {
			return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorNoScript,
				"no script to execute")
		}

		p, err := compiler.Compile(script.Plain)
		if err != nil {
			return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
				err.Error())
		}

		m := vm.NewMachine(*p)

		if err = m.SetVarsFromJSON(script.Vars); err != nil {
			return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(err, "could not set variables").Error())
		}

		resourcesChan, err := m.ResolveResources()
		if err != nil {
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"could not resolve program resources")
		}
		for req := range resourcesChan {
			if req.Error != nil {
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program resources").Error())
			}
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return []core.ExpandedTransaction{}, errors.Wrap(err,
					fmt.Sprintf("could not get account %q", req.Account))
			}
			if req.Key != "" {
				entry, ok := account.Metadata[req.Key]
				if !ok {
					return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
						fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
				}
				data, err := json.Marshal(entry)
				if err != nil {
					return []core.ExpandedTransaction{}, errors.Wrap(err, "json.Marshal")
				}
				value, err := machine.NewValueFromTypedJSON(data)
				if err != nil {
					return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
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
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(err, fmt.Sprintf("invalid ResourceRequest: %+v", req)).Error())
			}
		}

		balanceCh, err := m.ResolveBalances()
		if err != nil {
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"could not resolve balances")
		}
		for req := range balanceCh {
			if req.Error != nil {
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program balances").Error())
			}
			account, err := l.GetAccount(ctx, req.Account)
			if err != nil {
				return []core.ExpandedTransaction{}, errors.Wrap(err,
					fmt.Sprintf("could not get account %q", req.Account))
			}
			amt := account.Balances[req.Asset].OrZero()
			resp := machine.MonetaryInt(*amt)
			req.Response <- &resp
		}

		exitCode, err := m.Execute()
		if err != nil {
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"script execution failed")
		}

		if exitCode != vm.EXIT_OK {
			switch exitCode {
			case vm.EXIT_FAIL:
				return []core.ExpandedTransaction{}, errors.New(
					"script exited with error code EXIT_FAIL")
			case vm.EXIT_FAIL_INVALID:
				return []core.ExpandedTransaction{}, errors.New(
					"internal error: compiled script was invalid")
			case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
				// TODO: If the machine can provide the asset which is failing
				// we should be able to use InsufficientFundError{} instead of error code
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorInsufficientFund,
					"account had insufficient funds")
			default:
				return []core.ExpandedTransaction{}, errors.New(
					"script execution failed")
			}
		}

		metadata := m.GetTxMetaJSON()
		for k, v := range metadata {
			asMapAny := make(map[string]any)
			if err := json.Unmarshal(v.([]byte), &asMapAny); err != nil {
				return []core.ExpandedTransaction{}, errors.Wrap(err, "json.Unmarshal")
			}
			metadata[k] = asMapAny
		}
		for k, v := range script.Metadata {
			_, ok := metadata[k]
			if ok {
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorMetadataOverride,
					"cannot override metadata from script")
			}
			metadata[k] = v
		}

		for account, meta := range m.GetAccountsMetaJSON() {
			meta := meta.(map[string][]byte)
			for k, v := range meta {
				asMapAny := make(map[string]any)
				if err := json.Unmarshal(v, &asMapAny); err != nil {
					return []core.ExpandedTransaction{}, errors.Wrap(err, "json.Unmarshal")
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

		if len(m.Postings) == 0 {
			return []core.ExpandedTransaction{},
				NewValidationError("transaction has no postings")
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

	var nextTxId uint64
	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return []core.ExpandedTransaction{}, errors.Wrap(err,
			"could not get last transaction")
	}
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
	}

	contracts := make([]core.Contract, 0)
	if checkMapping {
		mapping, err := l.store.LoadMapping(ctx)
		if err != nil {
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"loading mapping")
		}
		if mapping != nil {
			contracts = append(contracts, mapping.Contracts...)
		}
		contracts = append(contracts, DefaultContracts...)
	}

	vAggr := NewVolumeAggregator(l.store)

	txs := make([]core.ExpandedTransaction, 0)

	usedReferences := make(map[string]struct{})
	for i, txData := range txsData {
		if txData.Timestamp.IsZero() {
			txData.Timestamp = time.Now().UTC().Truncate(time.Second)
		}

		past := false
		if lastTx != nil && txData.Timestamp.Before(lastTx.Timestamp) {
			past = true
		}
		if past && !l.allowPastTimestamps {
			return []core.ExpandedTransaction{}, NewValidationError(fmt.Sprintf(
				"cannot pass a timestamp prior to the last transaction: %s (passed) is %s before %s (last)",
				txData.Timestamp.Format(time.RFC3339Nano), lastTx.Timestamp.Sub(txData.Timestamp), lastTx.Timestamp.Format(time.RFC3339Nano)))
		}

		if txData.Reference != "" {
			if _, ok := usedReferences[txData.Reference]; ok {
				return []core.ExpandedTransaction{}, NewConflictError()
			}
			usedReferences[txData.Reference] = struct{}{}
		}

		txVolumeAggregator := vAggr.NextTx()

		for _, p := range txData.Postings {
			if err := txVolumeAggregator.Transfer(ctx, p.Source, p.Destination, p.Asset, p.Amount); err != nil {
				return []core.ExpandedTransaction{}, NewTransactionCommitError(i, err)
			}
		}

		accounts := make(map[string]*core.Account, 0)
		for addr, volumes := range txVolumeAggregator.PostCommitVolumes() {
			for asset, volume := range volumes {
				if addr == "world" {
					continue
				}

				for _, contract := range contracts {
					if contract.Match(addr) {
						if _, ok := accounts[addr]; !ok {
							account, err := l.store.GetAccount(ctx, addr)
							if err != nil {
								return []core.ExpandedTransaction{}, NewTransactionCommitError(i,
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
							return []core.ExpandedTransaction{}, NewInsufficientFundError(asset)
						}
						break
					}
				}
			}
		}

		tx := core.ExpandedTransaction{
			Transaction: core.Transaction{
				TransactionData: txData,
				ID:              nextTxId,
			},
			PostCommitVolumes: txVolumeAggregator.PostCommitVolumes(),
			PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes(),
		}
		lastTx = &tx
		txs = append(txs, tx)
		nextTxId++
	}

	if preview {
		return txs, nil
	}

	if err := l.store.Commit(ctx, txs...); err != nil {
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			return []core.ExpandedTransaction{}, NewConflictError()
		default:
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"committing transactions")
		}
	}

	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			if err := l.store.UpdateAccountMetadata(ctx,
				addr, m, time.Now().Round(time.Second).UTC()); err != nil {
				return []core.ExpandedTransaction{}, errors.Wrap(err,
					"updating account metadata")
			}
		}
	}

	l.monitor.CommittedTransactions(ctx, l.store.Name(), CommitResult{
		PreCommitVolumes:      vAggr.AggregatedPreCommitVolumes(),
		PostCommitVolumes:     vAggr.AggregatedPostCommitVolumes(),
		GeneratedTransactions: txs,
	})
	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			l.monitor.SavedMetadata(ctx,
				l.store.Name(), core.MetaTargetTypeAccount, addr, m)
		}
	}

	return txs, nil
}
