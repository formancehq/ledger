package runner

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/machine"
	"github.com/formancehq/ledger/pkg/machine/script/compiler"
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
}

func (r *Runner) GetMoreRecentTransactionDate() core.Time {
	return r.lastTransactionDate
}

func (r *Runner) Execute(
	ctx context.Context,
	script core.RunScript,
	dryRun bool,
	logComputer func(transaction core.ExpandedTransaction, accountMetadata map[string]core.Metadata) core.Log,
) (*core.ExpandedTransaction, core.Log, error) {

	inFlight, err := r.acquireInflight(ctx, script)
	if err != nil {
		return nil, core.Log{}, err
	}
	defer r.releaseInFlight(inFlight)

	transaction, accountMetadata, err := r.execute(ctx, script, dryRun)
	if err != nil {
		return nil, core.Log{}, err
	}
	if dryRun {
		return transaction, core.Log{}, err
	}

	log := logComputer(*transaction, accountMetadata).WithReference(script.Reference)
	if err := r.store.AppendLog(ctx, &log); err != nil {
		return nil, core.Log{}, err
	}

	r.releaseInFlightWithTransaction(inFlight, &transaction.Transaction)

	return transaction, log, nil
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
		if err != sql.ErrNoRows {
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

func (r *Runner) execute(ctx context.Context, script core.RunScript, dryRun bool) (*core.ExpandedTransaction, map[string]core.Metadata, error) {

	prog, err := compiler.Compile(script.Plain)
	if err != nil {
		return nil, nil, vm.NewScriptError(vm.ScriptErrorCompilationFailed, errors.Wrap(err, "compiling numscript").Error())
	}

	involvedAccounts, err := prog.GetInvolvedAccounts(script.Vars)
	if err != nil {
		return nil, nil, vm.NewScriptError(vm.ScriptErrorCompilationFailed, err.Error())
	}

	unlock, err := r.locker.Lock(ctx, r.store.Name(), involvedAccounts...)
	if err != nil {
		panic(err)
	}
	defer unlock(context.Background()) // Use a background context instead of the request one as it could have been cancelled

	result, err := machine.Run(ctx, r.cache, prog, script)
	if err != nil {
		return nil, nil, err
	}

	if len(result.Postings) == 0 {
		return nil, nil, NewValidationError("transaction has no postings")
	}

	vAggr := aggregator.Volumes(r.cache)
	txVolumeAggr, err := vAggr.NextTxWithPostings(ctx, result.Postings...)
	if err != nil {
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
		return expandedTx, result.AccountMetadata, nil
	}

	r.cache.UpdateVolumeWithTX(expandedTx.Transaction)

	return expandedTx, result.AccountMetadata, nil
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

func New(store storage.LedgerStore, locker lock.Locker, cache *cache.Cache, allowPastTimestamps bool) (*Runner, error) {
	log, err := store.ReadLastLogWithType(context.Background(), core.NewTransactionLogType, core.RevertedTransactionLogType)
	if err != nil && err != sql.ErrNoRows {
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
	}, nil
}
