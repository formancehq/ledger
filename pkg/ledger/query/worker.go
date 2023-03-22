package query

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

var (
	DefaultWorkerConfig = workerConfig{
		ChanSize: 100,
	}
)

type workerConfig struct {
	ChanSize int
}

type workerLog struct {
	log   core.Log
	store storage.LedgerStore
}

type Worker struct {
	workerConfig
	ctx                context.Context
	logChan            chan workerLog
	stopChan           chan chan struct{}
	driver             storage.Driver
	monitor            Monitor
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

func (w *Worker) run() error {
	if err := w.initLedgers(w.ctx); err != nil {
		if err == context.Canceled {
			logging.FromContext(w.ctx).Debugf("CQRS worker canceled")
		} else {
			logging.FromContext(w.ctx).Errorf("CQRS worker error: %s", err)
		}

		return err
	}

	for {
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case stopChan := <-w.stopChan:
			logging.FromContext(w.ctx).Debugf("CQRS worker stopped")
			close(stopChan)
			return nil
		case wl := <-w.logChan:
			if w.lastProcessedLogID != nil && wl.log.ID <= *w.lastProcessedLogID {
				continue
			}
			if err := w.processLog(w.ctx, wl.store, wl.log); err != nil {
				if err == context.Canceled {
					logging.FromContext(w.ctx).Debugf("CQRS worker canceled")
				} else {
					logging.FromContext(w.ctx).Errorf("CQRS worker error: %s", err)
				}

				// Return the error to restart the worker
				return err
			}

			if err := wl.store.UpdateNextLogID(w.ctx, wl.log.ID+1); err != nil {
				logging.FromContext(w.ctx).Errorf("CQRS worker error: %s", err)

				// TODO(polo/gfyrag): add indempotency tests
				// Return the error to restart the worker
				return err
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
	if err != nil {
		return errors.Wrap(err, "reading last log")
	}

	logs, err := store.ReadLogsStartingFromID(ctx, lastReadLogID)
	if err != nil {
		return errors.Wrap(err, "reading logs since last ID")
	}

	if len(logs) == 0 {
		return nil
	}

	if err := w.processLogs(ctx, store, logs...); err != nil {
		return errors.Wrap(err, "processing logs")
	}

	if err := store.UpdateNextLogID(ctx, logs[len(logs)-1].ID+1); err != nil {
		return errors.Wrap(err, "updating last read log")
	}
	lastProcessedLogID := logs[len(logs)-1].ID
	w.lastProcessedLogID = &lastProcessedLogID

	return nil
}

func (w *Worker) processLogs(ctx context.Context, store storage.LedgerStore, logs ...core.Log) error {
	for _, log := range logs {
		if err := w.processLog(ctx, store, log); err != nil {
			return errors.Wrapf(err, "processing log %d", log.ID)
		}
	}

	return nil
}

func (w *Worker) processLog(ctx context.Context, store storage.LedgerStore, log core.Log) error {
	volumeAggregator := aggregator.Volumes(store)

	var err error
	switch log.Type {
	case core.NewTransactionLogType:
		payload := log.Data.(core.NewTransactionLogPayload)
		txVolumeAggregator, err := volumeAggregator.NextTxWithPostings(ctx, payload.Transaction.Postings...)
		if err != nil {
			return err
		}

		if payload.AccountMetadata != nil {
			for account, metadata := range payload.AccountMetadata {
				if err := store.UpdateAccountMetadata(ctx, account, metadata); err != nil {
					return errors.Wrap(err, "updating account metadata")
				}
			}
		}

		expandedTx := core.ExpandedTransaction{
			Transaction:       payload.Transaction,
			PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes,
			PostCommitVolumes: txVolumeAggregator.PostCommitVolumes,
		}

		if err := store.InsertTransactions(ctx, expandedTx); err != nil {
			return errors.Wrap(err, "inserting transactions")
		}

		for account := range txVolumeAggregator.PostCommitVolumes {
			if err := store.EnsureAccountExists(ctx, account); err != nil {
				return errors.Wrap(err, "ensuring account exists")
			}
		}

		if err := store.UpdateVolumes(ctx, txVolumeAggregator.PostCommitVolumes); err != nil {
			return errors.Wrap(err, "updating volumes")
		}

		if w.monitor != nil {
			w.monitor.CommittedTransactions(ctx, store.Name(), expandedTx)
			for account, metadata := range payload.AccountMetadata {
				w.monitor.SavedMetadata(ctx, store.Name(), core.MetaTargetTypeAccount, account, metadata)
			}
		}

	case core.SetMetadataLogType:
		setMetadata := log.Data.(core.SetMetadataLogPayload)
		switch setMetadata.TargetType {
		case core.MetaTargetTypeAccount:
			if err := store.UpdateAccountMetadata(ctx, setMetadata.TargetID.(string), setMetadata.Metadata); err != nil {
				return errors.Wrap(err, "updating account metadata")
			}
		case core.MetaTargetTypeTransaction:
			if err := store.UpdateTransactionMetadata(ctx, setMetadata.TargetID.(uint64), setMetadata.Metadata); err != nil {
				return errors.Wrap(err, "updating transactions metadata")
			}
		}
		if w.monitor != nil {
			w.monitor.SavedMetadata(ctx, store.Name(), store.Name(), fmt.Sprint(setMetadata.TargetID), setMetadata.Metadata)
		}
	case core.RevertedTransactionLogType:
		payload := log.Data.(core.RevertedTransactionLogPayload)
		if err := store.UpdateTransactionMetadata(ctx, payload.RevertedTransactionID,
			core.RevertedMetadata(payload.RevertTransaction.ID)); err != nil {
			return errors.Wrap(err, "updating metadata")
		}
		txVolumeAggregator, err := volumeAggregator.NextTxWithPostings(ctx, payload.RevertTransaction.Postings...)
		if err != nil {
			return errors.Wrap(err, "aggregating volumes")
		}

		expandedTx := core.ExpandedTransaction{
			Transaction:       payload.RevertTransaction,
			PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes,
			PostCommitVolumes: txVolumeAggregator.PostCommitVolumes,
		}
		if err := store.InsertTransactions(ctx, expandedTx); err != nil {
			return errors.Wrap(err, "inserting transaction")
		}

		if w.monitor != nil {
			revertedTx, err := store.GetTransaction(ctx, payload.RevertedTransactionID)
			if err != nil {
				return err
			}
			w.monitor.RevertedTransaction(ctx, store.Name(), revertedTx, &expandedTx)
		}
	}

	return err
}

func (w *Worker) QueueLog(ctx context.Context, log core.Log, store storage.LedgerStore) {
	select {
	case <-w.ctx.Done():
		return
	case w.logChan <- workerLog{log, store}:
	}
}

func NewWorker(config workerConfig, driver storage.Driver, monitor Monitor) *Worker {
	return &Worker{
		logChan:      make(chan workerLog, config.ChanSize),
		stopChan:     make(chan chan struct{}),
		workerConfig: config,
		driver:       driver,
		monitor:      monitor,
	}
}
