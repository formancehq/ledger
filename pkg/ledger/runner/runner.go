package runner

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/numscript"
	"github.com/formancehq/ledger/pkg/ledger/state"
	"github.com/formancehq/ledger/pkg/machine"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Store interface {
	AppendLog(ctx context.Context, log *core.Log) error
	ReadLastLogWithType(background context.Context, logType ...core.LogType) (*core.Log, error)
	ReadLogWithReference(ctx context.Context, reference string) (*core.Log, error)
}

type Cache interface {
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
	LockAccounts(ctx context.Context, accounts ...string) (cache.Release, error)
	UpdateVolumeWithTX(transaction core.Transaction)
}

type Runner struct {
	store Store
	// cache is used to store accounts
	cache Cache
	// nextTxID store the next transaction id to be used
	nextTxID *atomic.Uint64
	// locker is used to local a set of account
	locker     *lock.Locker
	compiler   *numscript.Compiler
	state      *state.State
	ledgerName string
}

type logComputer func(transaction core.ExpandedTransaction, accountMetadata map[string]metadata.Metadata) core.Log

func (r *Runner) Execute(
	ctx context.Context,
	script core.RunScript,
	dryRun bool,
	logComputer logComputer,
) (*core.ExpandedTransaction, *core.LogHolder, error) {

	if script.Plain == "" {
		return nil, nil, ErrNoScript
	}

	reserve, ts, err := r.state.Reserve(ctx, state.ReserveRequest{
		Timestamp: script.Timestamp,
		Reference: script.Reference,
	})
	if err != nil {
		return nil, nil, errorsutil.NewError(ErrState, err)
	}
	defer reserve.Clear(nil)

	script.Timestamp = *ts

	transaction, logHolder, err := r.execute(ctx, script, logComputer, dryRun)
	if err != nil {
		return nil, nil, err
	}

	if dryRun {
		return transaction, nil, err
	}

	if err := r.store.AppendLog(ctx, logHolder.Log); err != nil {
		return nil, nil, errors.Wrap(err, "appending log")
	}

	reserve.Clear(&transaction.Transaction)

	return transaction, logHolder, nil
}

func (r *Runner) execute(ctx context.Context, script core.RunScript, logComputer logComputer, dryRun bool) (*core.ExpandedTransaction, *core.LogHolder, error) {
	program, err := r.compiler.Compile(ctx, script.Plain)
	if err != nil {
		return nil, nil, errorsutil.NewError(ErrCompilationFailed, errors.Wrap(err, "compiling numscript"))
	}

	m := vm.NewMachine(*program)

	if err := m.SetVarsFromJSON(script.Vars); err != nil {
		return nil, nil, errorsutil.NewError(ErrCompilationFailed,
			errors.Wrap(err, "could not set variables"))
	}

	involvedAccounts, involvedSources, err := m.ResolveResources(ctx, r.cache)
	if err != nil {
		return nil, nil, errorsutil.NewError(ErrCompilationFailed,
			errors.Wrap(err, "could not resolve program resources"))
	}

	// TODO: need to release even if an error is returned later
	release, err := r.cache.LockAccounts(ctx, involvedAccounts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "locking accounts")
	}

	lockAccounts := lock.Accounts{}
	for _, account := range involvedAccounts {
		if account == "world" {
			continue
		}
		lockAccounts.Read = append(lockAccounts.Read, account)
	}
	for _, account := range involvedSources {
		if account == "world" {
			continue
		}
		lockAccounts.Write = append(lockAccounts.Write, account)
	}

	unlock, err := r.locker.Lock(ctx, lockAccounts)
	if err != nil {
		release()
		return nil, nil, errors.Wrap(err, "locking accounts")
	}
	defer unlock(context.Background())

	err = m.ResolveBalances(ctx, r.cache)
	if err != nil {
		release()
		return nil, nil, errorsutil.NewError(ErrCompilationFailed,
			errors.Wrap(err, "could not resolve balances"))
	}

	result, err := machine.Run(m, script)
	if err != nil {
		release()
		return nil, nil, errors.Wrap(err, "running numscript")
	}

	if len(result.Postings) == 0 {
		release()
		return nil, nil, ErrNoPostings
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

func (r *Runner) GetState() *state.State {
	return r.state
}

func New(store Store, locker *lock.Locker, cache Cache, compiler *numscript.Compiler, ledgerName string, allowPastTimestamps bool) (*Runner, error) {
	log, err := store.ReadLastLogWithType(context.Background(), core.NewTransactionLogType, core.RevertedTransactionLogType)
	if err != nil && !storage.IsNotFoundError(err) {
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
		state:      state.New(store, allowPastTimestamps, lastTransactionDate),
		store:      store,
		cache:      cache,
		locker:     locker,
		nextTxID:   nextTxID,
		compiler:   compiler,
		ledgerName: ledgerName,
	}, nil
}
