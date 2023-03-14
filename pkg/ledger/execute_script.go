package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DmitriyVTitov/size"
	"github.com/dgraph-io/ristretto"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
	"github.com/formancehq/ledger/pkg/opentelemetry"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (l *Ledger) ProcessScript(ctx context.Context, writeLogs, preview bool, script core.ScriptData) (core.ExpandedTransaction, *Logs, error) {

	unlock, err := l.locker.Lock(ctx, l.store.Name())
	if err != nil {
		panic(err)
	}
	defer unlock(context.Background()) // Use a background context instead of the request one as it could have been cancelled

	ctx, span := opentelemetry.Start(ctx, "ExecuteScript")
	defer span.End()

	addOps := new(core.AdditionalOperations)

	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return core.ExpandedTransaction{}, nil,
			errors.Wrap(err, "could not get last transaction")
	}

	vAggr := NewVolumeAggregator(l)
	var nextTxId uint64
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
	}

	accs := map[string]*core.AccountWithVolumes{}
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
		return core.ExpandedTransaction{}, nil,
			NewValidationError(fmt.Sprintf(
				"cannot pass a timestamp prior to the last transaction: %s (passed) is %s before %s (last)",
				script.Timestamp.Format(time.RFC3339Nano),
				lastTx.Timestamp.Sub(script.Timestamp),
				lastTx.Timestamp.Format(time.RFC3339Nano)))
	}

	if script.Reference != "" {
		txs, err := l.GetTransactions(ctx, *NewTransactionsQuery().
			WithReferenceFilter(script.Reference))
		if err != nil {
			return core.ExpandedTransaction{}, nil,
				errors.Wrap(err,
					"get transactions with reference")
		}
		if len(txs.Data) > 0 {
			return core.ExpandedTransaction{}, nil, NewConflictError()
		}
	}

	if script.Plain == "" {
		return core.ExpandedTransaction{}, nil,
			NewScriptError(ScriptErrorNoScript, "no script to execute")
	}

	m, err := NewMachineFromScript(script.Plain, l.cache, span)
	if err != nil {
		return core.ExpandedTransaction{}, nil,
			NewScriptError(ScriptErrorCompilationFailed,
				err.Error())
	}

	if err := m.SetVarsFromJSON(script.Vars); err != nil {
		return core.ExpandedTransaction{}, nil,
			NewScriptError(ScriptErrorCompilationFailed,
				errors.Wrap(err, "could not set variables").Error())
	}

	resourcesChan, err := m.ResolveResources()
	if err != nil {
		return core.ExpandedTransaction{}, nil,
			errors.Wrap(err, "could not resolve program resources")
	}
	for req := range resourcesChan {
		if req.Error != nil {
			return core.ExpandedTransaction{}, nil,
				NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program resources").Error())
		}
		if _, ok := accs[req.Account]; !ok {
			accs[req.Account], err = l.GetAccount(ctx, req.Account)
			if err != nil {
				return core.ExpandedTransaction{}, nil,
					errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
			}
		}
		if req.Key != "" {
			entry, ok := accs[req.Account].Metadata[req.Key]
			if !ok {
				return core.ExpandedTransaction{}, nil,
					NewScriptError(ScriptErrorCompilationFailed,
						fmt.Sprintf("missing key %v in metadata for account %v", req.Key, req.Account))
			}
			data, err := json.Marshal(entry)
			if err != nil {
				return core.ExpandedTransaction{}, nil, errors.Wrap(err, "marshaling metadata")
			}
			value, err := core.NewValueFromTypedJSON(data)
			if err != nil {
				return core.ExpandedTransaction{}, nil,
					NewScriptError(ScriptErrorCompilationFailed,
						errors.Wrap(err, fmt.Sprintf(
							"invalid format for metadata at key %v for account %v",
							req.Key, req.Account)).Error())
			}
			req.Response <- *value
		} else if req.Asset != "" {
			amt := accs[req.Account].Balances[req.Asset].OrZero()
			resp := *amt
			req.Response <- &resp
		} else {
			return core.ExpandedTransaction{}, nil,
				NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(err, fmt.Sprintf("invalid ResourceRequest: %+v", req)).Error())
		}
	}

	balanceCh, err := m.ResolveBalances()
	if err != nil {
		return core.ExpandedTransaction{}, nil,
			errors.Wrap(err, "could not resolve balances")
	}
	for req := range balanceCh {
		if req.Error != nil {
			return core.ExpandedTransaction{}, nil,
				NewScriptError(ScriptErrorCompilationFailed,
					errors.Wrap(req.Error, "could not resolve program balances").Error())
		}
		var amt *core.MonetaryInt
		if _, ok := accs[req.Account]; !ok {
			accs[req.Account], err = l.GetAccount(ctx, req.Account)
			if err != nil {
				return core.ExpandedTransaction{}, nil,
					errors.Wrap(err,
						fmt.Sprintf("could not get account %q", req.Account))
			}
		}
		amt = accs[req.Account].Balances[req.Asset].OrZero()
		resp := *amt
		req.Response <- &resp
	}

	exitCode, err := m.Execute()
	if err != nil {
		return core.ExpandedTransaction{}, nil,
			errors.Wrap(err, "script execution failed")
	}

	if exitCode != vm.EXIT_OK {
		switch exitCode {
		case vm.EXIT_FAIL:
			return core.ExpandedTransaction{}, nil,
				errors.New("script exited with error code EXIT_FAIL")
		case vm.EXIT_FAIL_INVALID:
			return core.ExpandedTransaction{}, nil,
				errors.New("internal error: compiled script was invalid")
		case vm.EXIT_FAIL_INSUFFICIENT_FUNDS:
			// TODO: If the machine can provide the asset which is failing
			// we should be able to use InsufficientFundError{} instead of error code
			return core.ExpandedTransaction{}, nil,
				NewScriptError(ScriptErrorInsufficientFund,
					"account had insufficient funds")
		default:
			return core.ExpandedTransaction{}, nil,
				errors.New("script execution failed")
		}
	}

	if len(m.Postings) == 0 {
		return core.ExpandedTransaction{}, nil,
			NewValidationError("transaction has no postings")
	}

	txVolumeAggr := vAggr.NextTx()
	postings := make([]core.Posting, len(m.Postings))
	for j, posting := range m.Postings {
		amt := core.MonetaryInt(*posting.Amount)
		if err := txVolumeAggr.Transfer(ctx,
			posting.Source, posting.Destination, posting.Asset, &amt, accs); err != nil {
			return core.ExpandedTransaction{}, nil, errors.Wrap(err, "transferring volumes")
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
				return core.ExpandedTransaction{}, nil, errors.Wrap(err, fmt.Sprintf("get account '%s'", account))
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
			return core.ExpandedTransaction{}, nil, errors.Wrap(err, "unmarshaling transaction metadata")
		}
		metadata[k] = asMapAny
	}
	for k, v := range script.Metadata {
		_, ok := metadata[k]
		if ok {
			return core.ExpandedTransaction{}, nil,
				NewScriptError(ScriptErrorMetadataOverride,
					"cannot override metadata from script")
		}
		metadata[k] = v
	}

	for account, meta := range m.GetAccountsMetaJSON() {
		meta := meta.(map[string][]byte)
		for k, v := range meta {
			asMapAny := make(map[string]any)
			if err := json.Unmarshal(v, &asMapAny); err != nil {
				return core.ExpandedTransaction{}, nil, errors.Wrap(err, "unmarshaling account metadata")
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

	if preview {
		return tx, &Logs{}, nil
	}

	if err := l.store.Commit(ctx, tx); err != nil {
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			return core.ExpandedTransaction{}, nil, NewConflictError()
		default:
			return core.ExpandedTransaction{}, nil,
				errors.Wrap(err, "committing transactions")
		}
	}

	ls := make([]core.Log, 0)
	ls = append(ls, core.NewTransactionLog(tx.Transaction))

	if addOps != nil && addOps.SetAccountMeta != nil {
		for addr, m := range addOps.SetAccountMeta {
			at := time.Now().Round(time.Second).UTC()
			if err := l.store.UpdateAccountMetadata(ctx, addr, m); err != nil {
				return core.ExpandedTransaction{}, nil,
					errors.Wrap(err, "updating account metadata")
			}
			ls = append(ls, core.NewSetMetadataLog(at, core.SetMetadata{
				TargetType: core.MetaTargetTypeAccount,
				TargetID:   addr,
				Metadata:   m,
			}))
		}
	}

	logs := NewLogs(l.store.AppendLogs, ls, []postProcessing{func(ctx context.Context) error {
		l.monitor.CommittedTransactions(ctx, l.store.Name(), tx)
		if addOps != nil && addOps.SetAccountMeta != nil {
			for addr, m := range addOps.SetAccountMeta {
				l.monitor.SavedMetadata(ctx,
					l.store.Name(), core.MetaTargetTypeAccount, addr, m)
			}
		}

		return nil
	}})

	if writeLogs {
		if err := logs.Write(ctx); err != nil {
			return core.ExpandedTransaction{}, nil, errors.Wrap(err, "writing logs")
		}
	}

	return tx, logs, nil
}

func NewMachineFromScript(script string, cache *ristretto.Cache, span trace.Span) (*vm.Machine, error) {
	h := sha256.New()
	if _, err := h.Write([]byte(script)); err != nil {
		return nil, errors.Wrap(err, "hashing script")
	}
	curr := h.Sum(nil)

	if cachedProgram, found := cache.Get(curr); found {
		span.SetAttributes(attribute.Bool("numscript-cache-hit", true))
		return vm.NewMachine(cachedProgram.(program.Program)), nil
	}

	span.SetAttributes(attribute.Bool("numscript-cache-hit", false))
	prog, err := compiler.Compile(script)
	if err != nil {
		return nil, err
	}

	progSizeBytes := size.Of(*prog)
	if progSizeBytes == -1 {
		return nil, fmt.Errorf("error while calculating the size in bytes of the program")
	}
	cache.Set(curr, *prog, int64(progSizeBytes))

	return vm.NewMachine(*prog), nil
}
