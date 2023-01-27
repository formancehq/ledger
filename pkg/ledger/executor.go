package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DmitriyVTitov/size"
	machine "github.com/formancehq/machine/core"
	"github.com/formancehq/machine/script/compiler"
	"github.com/formancehq/machine/vm"
	"github.com/formancehq/machine/vm/program"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

func (l *Ledger) Execute(ctx context.Context, checkMapping, preview bool, scripts ...core.ScriptData) ([]core.ExpandedTransaction, error) {

	ctx, span := opentelemetry.Start(ctx, "Execute")
	defer span.End()

	if len(scripts) == 0 {
		return []core.ExpandedTransaction{},
			NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	addOps := new(core.AdditionalOperations)

	subContext, span := opentelemetry.Start(ctx, "Get last transaction")
	lastTx, err := l.store.GetLastTransaction(subContext)
	if err != nil {
		span.End()
		return []core.ExpandedTransaction{}, errors.Wrap(err,
			"could not get last transaction")
	}
	span.End()

	vAggr := NewVolumeAggregator(l)
	txs := make([]core.ExpandedTransaction, 0)
	var nextTxId uint64
	var lastTxTimestamp time.Time
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
		lastTxTimestamp = lastTx.Timestamp
	}
	contracts := make([]core.Contract, 0)
	if checkMapping {
		subContext, span := opentelemetry.Start(ctx, "Load mapping")
		mapping, err := l.store.LoadMapping(subContext)
		if err != nil {
			span.End()
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"loading mapping")
		}
		if mapping != nil {
			contracts = append(contracts, mapping.Contracts...)
		}
		contracts = append(contracts, DefaultContracts...)
		span.End()
	}

	usedReferences := make(map[string]struct{})
	accs := map[string]*core.AccountWithVolumes{}
	for i, script := range scripts {
		// Until v1.5.0, dates was stored as string using rfc3339 format
		// So round the date to the second to keep the same behaviour
		if script.Timestamp.IsZero() {
			script.Timestamp = time.Now().UTC().Truncate(time.Second)
		} else {
			script.Timestamp = script.Timestamp.UTC()
		}

		past := false
		if lastTx != nil && script.Timestamp.Before(lastTxTimestamp) {
			past = true
		}
		if past && !l.allowPastTimestamps {
			return []core.ExpandedTransaction{}, NewValidationError(fmt.Sprintf(
				"cannot pass a timestamp prior to the last transaction: %s (passed) is %s before %s (last)",
				script.Timestamp.Format(time.RFC3339Nano),
				lastTxTimestamp.Sub(script.Timestamp),
				lastTxTimestamp.Format(time.RFC3339Nano)))
		}
		lastTxTimestamp = script.Timestamp

		subContext, span := opentelemetry.Start(ctx, "Check reference")
		if script.Reference != "" {
			if _, ok := usedReferences[script.Reference]; ok {
				span.End()
				return []core.ExpandedTransaction{}, NewConflictError()
			}
			usedReferences[script.Reference] = struct{}{}

			txs, err := l.GetTransactions(subContext, *NewTransactionsQuery().
				WithReferenceFilter(script.Reference))
			if err != nil {
				span.End()
				return []core.ExpandedTransaction{}, errors.Wrap(err, "GetTransactions")
			}
			if len(txs.Data) > 0 {
				span.End()
				return []core.ExpandedTransaction{}, NewConflictError()
			}
		}
		span.End()

		if script.Plain == "" {
			return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorNoScript,
				"no script to execute")
		}

		_, span = opentelemetry.Start(ctx, "Compute hash")
		h := sha256.New()
		if _, err := h.Write([]byte(script.Plain)); err != nil {
			span.End()
			return []core.ExpandedTransaction{}, errors.Wrap(err, "hashing script")
		}
		curr := h.Sum(nil)
		span.End()

		var m *vm.Machine
		if cachedP, found := l.cache.Get(curr); found {
			//logging.Debugf("Ledger.Execute: Numscript found in cache: %x", curr)
			m = vm.NewMachine(cachedP.(program.Program))
		} else {
			_, span = opentelemetry.Start(ctx, "Compile numscript")
			newP, err := compiler.Compile(script.Plain)
			span.End()
			if err != nil {
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					err.Error())
			}
			s := size.Of(*newP)
			if s == -1 {
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					fmt.Errorf("error while calculating the size in bytes of script: %s",
						script.Plain).Error())
			}
			_, span = opentelemetry.Start(ctx, "Store cache value")
			ok := l.cache.Set(curr, *newP, int64(s))
			span.End()
			_ = ok
			//logging.Debugf("Ledger.Execute: Numscript NOT found in cache (size %d, set attempt returned %v): %x", s, ok, curr)
			m = vm.NewMachine(*newP)
		}

		if err := m.SetVarsFromJSON(script.Vars); err != nil {
			return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(err, "could not set variables").Error())
		}

		subContext, span = opentelemetry.Start(ctx, "Resolve resources")
		resourcesChan, err := m.ResolveResources()
		if err != nil {
			span.End()
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"could not resolve program resources")
		}
		for req := range resourcesChan {
			if req.Error != nil {
				span.End()
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program resources").Error())
			}
			if _, ok := accs[req.Account]; !ok {
				accs[req.Account], err = l.GetAccount(subContext, req.Account)
				if err != nil {
					span.End()
					return []core.ExpandedTransaction{}, errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
				}
			}
			if req.Key != "" {
				entry, ok := accs[req.Account].Metadata[req.Key]
				if !ok {
					span.End()
					return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
						fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
				}
				data, err := json.Marshal(entry)
				if err != nil {
					span.End()
					return []core.ExpandedTransaction{}, errors.Wrap(err, "json.Marshal")
				}
				value, err := machine.NewValueFromTypedJSON(data)
				if err != nil {
					span.End()
					return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
						errors.Wrap(err, fmt.Sprintf(
							"invalid format for metadata at key %v for account %v",
							req.Key, req.Account)).Error())
				}
				req.Response <- *value
			} else if req.Asset != "" {
				amt := accs[req.Account].Balances[req.Asset].OrZero()
				resp := machine.MonetaryInt(*amt)
				req.Response <- &resp
			} else {
				span.End()
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(err, fmt.Sprintf("invalid ResourceRequest: %+v", req)).Error())
			}
		}

		balanceCh, err := m.ResolveBalances()
		if err != nil {
			span.End()
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"could not resolve balances")
		}
		for req := range balanceCh {
			if req.Error != nil {
				span.End()
				return []core.ExpandedTransaction{}, NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program balances").Error())
			}
			var amt *core.MonetaryInt
			if _, ok := accs[req.Account]; !ok {
				accs[req.Account], err = l.GetAccount(subContext, req.Account)
				if err != nil {
					span.End()
					return []core.ExpandedTransaction{}, errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
				}
			}
			amt = accs[req.Account].Balances[req.Asset].OrZero()
			resp := machine.MonetaryInt(*amt)
			req.Response <- &resp
		}
		span.End()

		_, span = opentelemetry.Start(ctx, "Run machine")
		exitCode, err := m.Execute()
		if err != nil {
			span.End()
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"script execution failed")
		}
		span.End()

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

		if len(m.Postings) == 0 {
			return []core.ExpandedTransaction{},
				NewValidationError("transaction has no postings")
		}

		subContext, span = opentelemetry.Start(ctx, "Aggregate post/pre commit volumes")
		txVolumeAggr := vAggr.NextTx()
		postings := make([]core.Posting, len(m.Postings))
		for j, posting := range m.Postings {
			amt := core.MonetaryInt(*posting.Amount)
			if err := txVolumeAggr.Transfer(subContext,
				posting.Source, posting.Destination, posting.Asset, &amt, accs); err != nil {
				span.End()
				return []core.ExpandedTransaction{}, NewTransactionCommitError(i, err)
			}
			postings[j] = core.Posting{
				Source:      posting.Source,
				Destination: posting.Destination,
				Amount:      &amt,
				Asset:       posting.Asset,
			}
		}
		span.End()

		subContext, span = opentelemetry.Start(ctx, "Check business rules (mapping)")
		for account, volumes := range txVolumeAggr.PostCommitVolumes {
			if _, ok := accs[account]; !ok {
				accs[account], err = l.GetAccount(subContext, account)
				if err != nil {
					span.End()
					return []core.ExpandedTransaction{}, NewTransactionCommitError(i,
						errors.Wrap(err, fmt.Sprintf("GetAccount '%s'", account)))
				}
			}
			for asset, vol := range volumes {
				accs[account].Volumes[asset] = vol
			}
			accs[account].Balances = accs[account].Volumes.Balances()
			for asset, volume := range volumes {
				if account == core.WORLD {
					continue
				}

				for _, contract := range contracts {
					if contract.Match(account) {
						if ok := contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": volume.Balance(),
							},
							Metadata: accs[account].Metadata,
							Asset:    asset,
						}); !ok {
							span.End()
							return []core.ExpandedTransaction{}, NewInsufficientFundError(asset)
						}
						break
					}
				}
			}
		}
		span.End()

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
			PreCommitVolumes:  txVolumeAggr.PreCommitVolumes,
			PostCommitVolumes: txVolumeAggr.PostCommitVolumes,
		}
		lastTx = &tx
		txs = append(txs, tx)
		nextTxId++
	}

	if preview {
		return txs, nil
	}

	newContext, span := opentelemetry.Start(ctx, "Persist data")
	if err := l.store.Commit(newContext, txs...); err != nil {
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			span.End()
			return []core.ExpandedTransaction{}, NewConflictError()
		default:
			span.End()
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"committing transactions")
		}
	}

	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			if err := l.store.UpdateAccountMetadata(ctx,
				addr, m, time.Now().Round(time.Second).UTC()); err != nil {
				span.End()
				return []core.ExpandedTransaction{}, errors.Wrap(err,
					"updating account metadata")
			}
		}
	}
	span.End()

	ctx, span = opentelemetry.Start(ctx, "Fire events")
	l.monitor.CommittedTransactions(ctx, l.store.Name(), txs...)
	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			l.monitor.SavedMetadata(ctx,
				l.store.Name(), core.MetaTargetTypeAccount, addr, m)
		}
	}
	span.End()

	return txs, nil
}
