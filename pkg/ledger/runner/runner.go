package runner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/numscript"
	"github.com/formancehq/ledger/pkg/machine"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/pkg/errors"
)

type Runner struct {
	mu    sync.Mutex
	store storage.LedgerStore
	// cache is used to store accounts
	cache *cache.Cache
	// inFlights container in flight transactions
	inFlights map[*inFlight]struct{}
	// lastTransactionDate store the more recent processed transactions
	// the matching log could be written or not
	lastTransactionDate core.Time
	// nextTxID store the next transaction id to be used
	nextTxID *atomic.Uint64
	// locker is used to local a set of account
	locker lock.Locker
	// allowPastTimestamps allow to insert transactions in the past
	allowPastTimestamps bool
	compiler            *numscript.Compiler
}

func (r *Runner) GetMoreRecentTransactionDate() core.Time {
	return r.lastTransactionDate
}

type logComputer func(transaction core.ExpandedTransaction, accountMetadata map[string]core.Metadata) core.Log

func (r *Runner) Execute(
	ctx context.Context,
	script core.RunScript,
	dryRun bool,
	logComputer logComputer,
) (*core.ExpandedTransaction, *core.LogHolder, error) {
	inFlight, err := r.acquireInflight(ctx, script)
	if err != nil {
		return nil, nil, err
	}
	defer r.releaseInFlight(inFlight)

	transaction, logHolder, err := r.execute(ctx, script, logComputer, dryRun)
	if err != nil {
		return nil, nil, err
	}
	if dryRun {
		return transaction, nil, err
	}

	if err := r.store.AppendLog(ctx, logHolder.Log); err != nil {
		return nil, nil, err
	}

	r.releaseInFlightWithTransaction(inFlight, &transaction.Transaction)

	return transaction, logHolder, nil
}

func (r *Runner) checkConstraints(ctx context.Context, script core.RunScript) error {
	var validationError = func(date core.Time) error {
		return NewValidationError(fmt.Sprintf(
			"cannot pass a timestamp prior to the last transaction: %s (passed) is %s before %s (last)",
			script.Timestamp.Format(time.RFC3339Nano),
			date.Sub(script.Timestamp),
			date.Format(time.RFC3339Nano)))
	}
	for inFlight := range r.inFlights {
		if !r.allowPastTimestamps {
			if inFlight.timestamp.After(script.Timestamp) {
				return validationError(inFlight.timestamp)
			}
		}
		if inFlight.reference != "" && inFlight.reference == script.Reference {
			return NewConflictError("reference already used, in flight occurring")
		}
	}

	if !r.allowPastTimestamps {
		if r.lastTransactionDate.After(script.Timestamp) {
			return validationError(script.Timestamp)
		}
	}

	if script.Reference != "" {
		_, err := r.store.ReadLogWithReference(ctx, script.Reference)
		if err == nil {
			// Log found
			return NewConflictError("reference found in storage")
		}
		if !storage.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *Runner) acquireInflight(ctx context.Context, script core.RunScript) (*inFlight, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	script.WithDefaultValues()
	if script.Plain == "" {
		return nil, vm.NewScriptError(vm.ScriptErrorNoScript, "no script to execute")
	}

	if err := r.checkConstraints(ctx, script); err != nil {
		return nil, err
	}

	ret := &inFlight{
		reference: script.Reference,
		timestamp: script.Timestamp,
	}
	r.inFlights[ret] = struct{}{}

	return ret, nil
}

func (r *Runner) execute(ctx context.Context, script core.RunScript, logComputer logComputer, dryRun bool) (*core.ExpandedTransaction, *core.LogHolder, error) {

	program, err := r.compiler.Compile(ctx, script.Plain)
	if err != nil {
		return nil, nil, vm.NewScriptError(vm.ScriptErrorCompilationFailed, errors.Wrap(err, "compiling numscript").Error())
	}

	m := vm.NewMachine(*program)

	if err := m.SetVarsFromJSON(script.Vars); err != nil {
		return nil, nil, vm.NewScriptError(vm.ScriptErrorCompilationFailed,
			errors.Wrap(err, "could not set variables").Error())
	}

	involvedAccounts, _, err := m.ResolveResources(ctx, r.cache)
	if err != nil {
		return nil, nil, vm.NewScriptError(vm.ScriptErrorCompilationFailed,
			errors.Wrap(err, "could not resolve program resources").Error())
	}

	// TODO: need to release even if an error is returned later
	release, err := r.cache.LockAccounts(ctx, involvedAccounts...)
	if err != nil {
		return nil, nil, err
	}

	unlock, err := r.locker.Lock(ctx, r.store.Name(), involvedAccounts...)
	if err != nil {
		release()
		return nil, nil, err
	}
	defer unlock(context.Background())

	err = m.ResolveBalances(ctx, r.cache)
	if err != nil {
		release()
		return nil, nil, vm.NewScriptError(vm.ScriptErrorCompilationFailed,
			errors.Wrap(err, "could not resolve balances").Error())
	}

	result, err := machine.Run(m, script)
	if err != nil {
		release()
		return nil, nil, err
	}

	if len(result.Postings) == 0 {
		release()
		return nil, nil, NewValidationError("transaction has no postings")
	}

	vAggr := aggregator.Volumes(r.cache)
	txVolumeAggr, err := vAggr.NextTxWithPostings(ctx, result.Postings...)
	if err != nil {
		release()
		return nil, nil, errors.Wrap(err, "transferring volumes")
	}

	txID := r.nextTxID.Load()
	if !dryRun {
		defer func() {
			r.nextTxID.Add(1)
		}()
	}

	expandedTx := &core.ExpandedTransaction{
		Transaction: core.NewTransaction().
			WithPostings(result.Postings...).
			WithReference(script.Reference).
			WithMetadata(result.Metadata).
			WithTimestamp(script.Timestamp).
			WithID(txID),
		PreCommitVolumes:  txVolumeAggr.PreCommitVolumes,
		PostCommitVolumes: txVolumeAggr.PostCommitVolumes,
	}
	if dryRun {
		release()
		return expandedTx, nil, nil
	}

	r.cache.UpdateVolumeWithTX(expandedTx.Transaction)

	log := logComputer(*expandedTx, result.AccountMetadata)
	if script.Reference != "" {
		log = log.WithReference(script.Reference)
	}
	logHolder := core.NewLogHolder(&log)
	go func() {
		// TODO(gfyrag): We need the app context to be able to listen on it (we cannot listen on request one)
		<-logHolder.Ingested
		release()
	}()

	return expandedTx, logHolder, nil
}

func (r *Runner) removeInFlight(inFlight *inFlight) {
	delete(r.inFlights, inFlight)
}

func (r *Runner) releaseInFlight(inFlight *inFlight) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removeInFlight(inFlight)
}

func (r *Runner) releaseInFlightWithTransaction(inFlight *inFlight, transaction *core.Transaction) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removeInFlight(inFlight)
	if transaction.Timestamp.After(r.lastTransactionDate) {
		r.lastTransactionDate = transaction.Timestamp
	}
}

func New(store storage.LedgerStore, locker lock.Locker, cache *cache.Cache, compiler *numscript.Compiler, allowPastTimestamps bool) (*Runner, error) {
	log, err := store.ReadLastLogWithType(context.Background(), core.NewTransactionLogType, core.RevertedTransactionLogType)
	if err != nil && !storage.IsNotFound(err) {
		return nil, err
	}
	var (
		lastTxID            *uint64
		lastTransactionDate core.Time
	)
	if err == nil {
		switch payload := log.Data.(type) {
		case core.NewTransactionLogPayload:
			lastTxID = &payload.Transaction.ID
			lastTransactionDate = payload.Transaction.Timestamp
		case core.RevertedTransactionLogPayload:
			lastTxID = &payload.RevertTransaction.ID
			lastTransactionDate = payload.RevertTransaction.Timestamp
		default:
			panic(fmt.Sprintf("unhandled payload type: %T", payload))
		}
	}
	nextTxID := &atomic.Uint64{}
	if lastTxID != nil {
		nextTxID.Add(*lastTxID)
		nextTxID.Add(1)
	}
	return &Runner{
		store:               store,
		cache:               cache,
		inFlights:           map[*inFlight]struct{}{},
		locker:              locker,
		allowPastTimestamps: allowPastTimestamps,
		nextTxID:            nextTxID,
		lastTransactionDate: lastTransactionDate,
		compiler:            compiler,
	}, nil
}
