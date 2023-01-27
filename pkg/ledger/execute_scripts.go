package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DmitriyVTitov/size"
	"github.com/formancehq/go-libs/logging"
	machine "github.com/formancehq/machine/core"
	"github.com/formancehq/machine/script/compiler"
	"github.com/formancehq/machine/vm"
	"github.com/formancehq/machine/vm/program"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
)

func (l *Ledger) ExecuteScripts(ctx context.Context, preview bool, scripts ...core.ScriptData) ([]core.ExpandedTransaction, error) {
	ctx, span := opentelemetry.Start(ctx, "ExecuteScripts")
	defer span.End()

	if len(scripts) == 0 {
		return []core.ExpandedTransaction{},
			NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	addOps := new(core.AdditionalOperations)

	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return []core.ExpandedTransaction{}, errors.Wrap(err,
			"could not get last transaction")
	}

	vAggr := NewVolumeAggregator(l)
	txs := make([]core.ExpandedTransaction, 0)
	var nextTxId uint64
	var lastTxTimestamp time.Time
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
		lastTxTimestamp = lastTx.Timestamp
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

		if script.Reference != "" {
			if _, ok := usedReferences[script.Reference]; ok {
				return []core.ExpandedTransaction{}, NewConflictError()
			}
			usedReferences[script.Reference] = struct{}{}

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

		h := sha256.New()
		if _, err := h.Write([]byte(script.Plain)); err != nil {
			return []core.ExpandedTransaction{}, errors.Wrap(err, "hashing script")
		}
		curr := h.Sum(nil)

		var m *vm.Machine
		if cachedP, found := l.cache.Get(curr); found {
			logging.Debugf("Ledger.ExecuteScripts: Numscript found in cache: %x", curr)
			m = vm.NewMachine(cachedP.(program.Program))
		} else {
			compileStartAt := time.Now()
			newP, err := compiler.Compile(script.Plain)
			compileTerminatedAt := time.Now()
			span.SetAttributes(attribute.Int("compilation-duration", int(compileTerminatedAt.Sub(compileStartAt).Seconds())))
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
			ok := l.cache.Set(curr, *newP, int64(s))
			logging.Debugf("Ledger.ExecuteScripts: Numscript NOT found in cache (size %d, set attempt returned %v): %x", s, ok, curr)
			m = vm.NewMachine(*newP)
		}

		if err := m.SetVarsFromJSON(script.Vars); err != nil {
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
			if _, ok := accs[req.Account]; !ok {
				accs[req.Account], err = l.GetAccount(ctx, req.Account)
				if err != nil {
					return []core.ExpandedTransaction{}, errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
				}
			}
			if req.Key != "" {
				entry, ok := accs[req.Account].Metadata[req.Key]
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
				amt := accs[req.Account].Balances[req.Asset].OrZero()
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
			var amt *core.MonetaryInt
			if _, ok := accs[req.Account]; !ok {
				accs[req.Account], err = l.GetAccount(ctx, req.Account)
				if err != nil {
					return []core.ExpandedTransaction{}, errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
				}
			}
			amt = accs[req.Account].Balances[req.Asset].OrZero()
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

		if len(m.Postings) == 0 {
			return []core.ExpandedTransaction{},
				NewValidationError("transaction has no postings")
		}

		txVolumeAggr := vAggr.NextTx()
		postings := make([]core.Posting, len(m.Postings))
		for j, posting := range m.Postings {
			amt := core.MonetaryInt(*posting.Amount)
			if err := txVolumeAggr.Transfer(ctx,
				posting.Source, posting.Destination, posting.Asset, &amt, accs); err != nil {
				return []core.ExpandedTransaction{}, NewTransactionCommitError(i, err)
			}
			postings[j] = core.Posting{
				Source:      posting.Source,
				Destination: posting.Destination,
				Amount:      &amt,
				Asset:       posting.Asset,
			}
		}

		for account, volumes := range txVolumeAggr.PostCommitVolumes {
			if _, ok := accs[account]; !ok {
				accs[account], err = l.GetAccount(ctx, account)
				if err != nil {
					return []core.ExpandedTransaction{}, NewTransactionCommitError(i,
						errors.Wrap(err, fmt.Sprintf("GetAccount '%s'", account)))
				}
			}
			for asset, vol := range volumes {
				accs[account].Volumes[asset] = vol
			}
			accs[account].Balances = accs[account].Volumes.Balances()
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

	l.monitor.CommittedTransactions(ctx, l.store.Name(), txs...)
	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			l.monitor.SavedMetadata(ctx,
				l.store.Name(), core.MetaTargetTypeAccount, addr, m)
		}
	}

	return txs, nil
}
