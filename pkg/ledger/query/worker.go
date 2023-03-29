package query

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

var (
	DefaultWorkerConfig = WorkerConfig{
		ChanSize: 100,
	}
)

type WorkerConfig struct {
	ChanSize int
}

type logsData struct {
	accountsToUpdate     []core.Account
	ensureAccountsExist  []string
	transactionsToInsert []core.ExpandedTransaction
	transactionsToUpdate []core.TransactionWithMetadata
	volumesToUpdate      []core.AccountsAssetsVolumes
	monitors             []func(context.Context, monitor.Monitor)
}

type Worker struct {
	WorkerConfig
	ctx context.Context

	pending      []*core.LogHolder
	writeChannel chan *core.LogHolder
	jobs         chan []*core.LogHolder
	releasedJob  chan struct{}
	errorChan    chan error
	stopChan     chan chan struct{}

	driver             storage.Driver
	store              storage.LedgerStore
	monitor            monitor.Monitor
	lastProcessedLogID *uint64
}

func (w *Worker) Run(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Start CQRS worker")

	w.ctx = ctx

	for {
		select {
		case <-w.ctx.Done():
			// Stop the worker if the context is done
			return w.ctx.Err()
		default:
			if err := w.run(); err != nil {
				// TODO(polo): add metrics
				if err == context.Canceled {
					// Stop the worker if the context is canceled
					return err
				}

				// Restart the worker if there is an error
			} else {
				// No error was returned, it means the worker was stopped
				// using the stopChan, let's stop this loop too
				return nil
			}
		}
	}
}

func (w *Worker) writeLoop(ctx context.Context) {
	closeLogs := func(logs []*core.LogHolder) {
		for _, log := range logs {
			close(log.Ingested)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case w.releasedJob <- struct{}{}:

		case modelsHolder := <-w.jobs:
			logs := make([]core.Log, len(modelsHolder))
			for i, holder := range modelsHolder {
				logs[i] = *holder.Log
			}

			if err := w.processLogs(w.ctx, logs...); err != nil {
				if err == context.Canceled {
					logging.FromContext(w.ctx).Debugf("CQRS worker canceled")
				} else {
					logging.FromContext(w.ctx).Errorf("CQRS worker error: %s", err)
				}
				closeLogs(modelsHolder)

				// Return the error to restart the worker
				w.errorChan <- err
				return
			}

			if err := w.store.UpdateNextLogID(w.ctx, logs[len(logs)-1].ID+1); err != nil {
				logging.FromContext(w.ctx).Errorf("CQRS worker error: %s", err)
				closeLogs(modelsHolder)

				// TODO(polo/gfyrag): add indempotency tests
				// Return the error to restart the worker
				w.errorChan <- err
				return
			}

			closeLogs(modelsHolder)
		}
	}
}

func (w *Worker) run() error {
	if err := w.initLedgers(w.ctx); err != nil {
		if err == context.Canceled {
			logging.FromContext(w.ctx).Debugf("CQRS worker canceled")
		} else {
			logging.FromContext(w.ctx).Errorf("CQRS worker error: %s", err)
		}

		return err
	}

	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()

	go w.writeLoop(ctx)

l:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case stopChan := <-w.stopChan:
			logging.FromContext(ctx).Debugf("CQRS worker stopped")
			close(stopChan)
			return nil

		case err := <-w.errorChan:
			// In this case, we failed to write the models, so we need to
			// restart the worker
			logging.FromContext(ctx).Debugf("write loop error: %s", err)
			return err

		// At this level, the job is writting some models, just accumulate models in a buffer
		case wl := <-w.writeChannel:
			if w.lastProcessedLogID != nil && wl.Log.ID <= *w.lastProcessedLogID {
				close(wl.Ingested)
				continue
			}

			w.pending = append(w.pending, wl)
		case <-w.releasedJob:
			// There, write model job is not running, and we have pending models
			// So we can try to send pending to the job channel
			if len(w.pending) > 0 {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()

					case stopChan := <-w.stopChan:
						logging.FromContext(ctx).Debugf("CQRS worker stopped")
						close(stopChan)
						return nil

					case err := <-w.errorChan:
						logging.FromContext(ctx).Debugf("write loop error: %s", err)
						return err

					case w.jobs <- w.pending:
						w.pending = make([]*core.LogHolder, 0)
						continue l
					}
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()

			case stopChan := <-w.stopChan:
				logging.FromContext(ctx).Debugf("CQRS worker stopped")
				close(stopChan)
				return nil

			case err := <-w.errorChan:
				logging.FromContext(ctx).Debugf("write loop error: %s", err)
				return err

			// There, the job is waiting, and we don't have any pending models to write
			// so, wait for new models to write and send them directly to the job channel
			// We can not return to the main loop as w.releasedJob will be continuously notified by the job routine
			case mh := <-w.writeChannel:
				select {
				case <-ctx.Done():
					return ctx.Err()
				case stopChan := <-w.stopChan:
					close(stopChan)
					return nil
				case w.jobs <- []*core.LogHolder{mh}:
				}
			}
		}
	}
}

func (w *Worker) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.stopChan <- ch:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
	}

	return nil
}

func (w *Worker) initLedgers(ctx context.Context) error {
	ledgers, err := w.driver.GetSystemStore().ListLedgers(ctx)
	if err != nil {
		return err
	}

	for _, ledger := range ledgers {
		if err := w.initLedger(ctx, ledger); err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) initLedger(ctx context.Context, ledger string) error {
	store, _, err := w.driver.GetLedgerStore(ctx, ledger, false)
	if err != nil && err != storage.ErrLedgerStoreNotFound {
		return err
	}
	if err == storage.ErrLedgerStoreNotFound {
		return nil
	}

	if !store.IsInitialized() {
		return nil
	}

	lastReadLogID, err := store.GetNextLogID(ctx)
	if err != nil && !storage.IsNotFound(err) {
		return errors.Wrap(err, "reading last log")
	}

	logs, err := store.ReadLogsStartingFromID(ctx, lastReadLogID)
	if err != nil {
		return errors.Wrap(err, "reading logs since last ID")
	}

	if len(logs) == 0 {
		return nil
	}

	if err := w.processLogs(ctx, logs...); err != nil {
		return errors.Wrap(err, "processing logs")
	}

	if err := store.UpdateNextLogID(ctx, logs[len(logs)-1].ID+1); err != nil {
		return errors.Wrap(err, "updating last read log")
	}
	lastProcessedLogID := logs[len(logs)-1].ID
	w.lastProcessedLogID = &lastProcessedLogID

	return nil
}

func (w *Worker) processLogs(ctx context.Context, logs ...core.Log) error {

	logsData, err := w.buildData(ctx, logs...)
	if err != nil {
		return errors.Wrap(err, "building data")
	}

	if err := w.store.RunInTransaction(ctx, func(ctx context.Context, tx storage.LedgerStore) error {
		if len(logsData.accountsToUpdate) > 0 {
			if err := tx.UpdateAccountsMetadata(ctx, logsData.accountsToUpdate); err != nil {
				return errors.Wrap(err, "updating accounts metadata")
			}
		}

		if len(logsData.transactionsToInsert) > 0 {
			if err := tx.InsertTransactions(ctx, logsData.transactionsToInsert...); err != nil {
				return errors.Wrap(err, "inserting transactions")
			}
		}

		if len(logsData.transactionsToUpdate) > 0 {
			if err := tx.UpdateTransactionsMetadata(ctx, logsData.transactionsToUpdate...); err != nil {
				return errors.Wrap(err, "updating transactions")
			}
		}

		if len(logsData.ensureAccountsExist) > 0 {
			if err := tx.EnsureAccountsExist(ctx, logsData.ensureAccountsExist); err != nil {
				return errors.Wrap(err, "ensuring accounts exist")
			}
		}

		if len(logsData.volumesToUpdate) > 0 {
			return tx.UpdateVolumes(ctx, logsData.volumesToUpdate...)
		}

		return nil
	}); err != nil {
		return err
	}

	if w.monitor != nil {
		for _, monitor := range logsData.monitors {
			monitor(ctx, w.monitor)
		}
	}

	return nil
}

func (w *Worker) buildData(
	ctx context.Context,
	logs ...core.Log,
) (*logsData, error) {
	logsData := &logsData{}

	volumeAggregator := aggregator.Volumes(w.store)
	accountsToUpdate := make(map[string]core.Metadata)
	transactionsToUpdate := make(map[uint64]core.Metadata)
	for _, log := range logs {
		switch log.Type {
		case core.NewTransactionLogType:
			payload := log.Data.(core.NewTransactionLogPayload)
			txVolumeAggregator, err := volumeAggregator.NextTxWithPostings(ctx, payload.Transaction.Postings...)
			if err != nil {
				return nil, err
			}

			if payload.AccountMetadata != nil {
				for account, metadata := range payload.AccountMetadata {
					if m, ok := accountsToUpdate[account]; !ok {
						accountsToUpdate[account] = metadata
					} else {
						for k, v := range metadata {
							m[k] = v
						}
					}
				}
			}

			expandedTx := core.ExpandedTransaction{
				Transaction:       payload.Transaction,
				PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes,
				PostCommitVolumes: txVolumeAggregator.PostCommitVolumes,
			}

			logsData.transactionsToInsert = append(logsData.transactionsToInsert, expandedTx)

			for account := range txVolumeAggregator.PostCommitVolumes {
				logsData.ensureAccountsExist = append(logsData.ensureAccountsExist, account)
			}

			logsData.volumesToUpdate = append(logsData.volumesToUpdate, txVolumeAggregator.PostCommitVolumes)

			logsData.monitors = append(logsData.monitors, func(ctx context.Context, monitor monitor.Monitor) {
				w.monitor.CommittedTransactions(ctx, w.store.Name(), expandedTx)
				for account, metadata := range payload.AccountMetadata {
					w.monitor.SavedMetadata(ctx, w.store.Name(), core.MetaTargetTypeAccount, account, metadata)
				}
			})

		case core.SetMetadataLogType:
			setMetadata := log.Data.(core.SetMetadataLogPayload)
			switch setMetadata.TargetType {
			case core.MetaTargetTypeAccount:
				addr := setMetadata.TargetID.(string)
				if m, ok := accountsToUpdate[addr]; !ok {
					accountsToUpdate[addr] = setMetadata.Metadata
				} else {
					for k, v := range setMetadata.Metadata {
						m[k] = v
					}
				}

			case core.MetaTargetTypeTransaction:
				id := setMetadata.TargetID.(uint64)
				if m, ok := transactionsToUpdate[id]; !ok {
					transactionsToUpdate[id] = setMetadata.Metadata
				} else {
					for k, v := range setMetadata.Metadata {
						m[k] = v
					}
				}
			}

			logsData.monitors = append(logsData.monitors, func(ctx context.Context, monitor monitor.Monitor) {
				w.monitor.SavedMetadata(ctx, w.store.Name(), w.store.Name(), fmt.Sprint(setMetadata.TargetID), setMetadata.Metadata)
			})

		case core.RevertedTransactionLogType:
			payload := log.Data.(core.RevertedTransactionLogPayload)
			id := payload.RevertedTransactionID
			metadata := core.RevertedMetadata(payload.RevertTransaction.ID)
			if m, ok := transactionsToUpdate[id]; !ok {
				transactionsToUpdate[id] = metadata
			} else {
				for k, v := range metadata {
					m[k] = v
				}
			}

			txVolumeAggregator, err := volumeAggregator.NextTxWithPostings(ctx, payload.RevertTransaction.Postings...)
			if err != nil {
				return nil, errors.Wrap(err, "aggregating volumes")
			}

			expandedTx := core.ExpandedTransaction{
				Transaction:       payload.RevertTransaction,
				PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes,
				PostCommitVolumes: txVolumeAggregator.PostCommitVolumes,
			}
			logsData.transactionsToInsert = append(logsData.transactionsToInsert, expandedTx)

			revertedTx, err := w.store.GetTransaction(ctx, payload.RevertedTransactionID)
			if err != nil {
				return nil, err
			}

			logsData.monitors = append(logsData.monitors, func(ctx context.Context, monitor monitor.Monitor) {
				w.monitor.RevertedTransaction(ctx, w.store.Name(), revertedTx, &expandedTx)
			})
		}
	}

	for account, metadata := range accountsToUpdate {
		logsData.accountsToUpdate = append(logsData.accountsToUpdate, core.Account{
			Address:  account,
			Metadata: metadata,
		})
	}

	for transaction, metadata := range transactionsToUpdate {
		logsData.transactionsToUpdate = append(logsData.transactionsToUpdate, core.TransactionWithMetadata{
			ID:       transaction,
			Metadata: metadata,
		})
	}

	return logsData, nil
}

func (w *Worker) QueueLog(ctx context.Context, log *core.LogHolder, store storage.LedgerStore) {
	select {
	case <-w.ctx.Done():
	case w.writeChannel <- log:
	}
}

func NewWorker(config WorkerConfig, driver storage.Driver, store storage.LedgerStore, monitor monitor.Monitor) *Worker {
	return &Worker{
		pending:      make([]*core.LogHolder, 0),
		jobs:         make(chan []*core.LogHolder),
		releasedJob:  make(chan struct{}, 1),
		writeChannel: make(chan *core.LogHolder, config.ChanSize),
		errorChan:    make(chan error, 1),
		stopChan:     make(chan chan struct{}),
		WorkerConfig: config,
		store:        store,
		driver:       driver,
		monitor:      monitor,
	}
}
