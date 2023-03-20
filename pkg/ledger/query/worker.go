package query

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

type workerConfig struct {
	Interval time.Duration
}

type Worker struct {
	workerConfig
	stopChan chan chan struct{}
	driver   storage.Driver
	monitor  Monitor
}

func (w *Worker) Run(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Start CQRS worker")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case stopChan := <-w.stopChan:
			logging.FromContext(ctx).Debugf("CQRS worker stopped")
			close(stopChan)
			return nil
		case <-time.After(w.Interval):
			if err := w.run(ctx); err != nil {
				if err == context.Canceled {
					logging.FromContext(ctx).Debugf("CQRS worker canceled")
					return err
				}
				logging.FromContext(ctx).Errorf("CQRS worker error: %s", err)
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

func (w *Worker) run(ctx context.Context) error {

	ledgers, err := w.driver.GetSystemStore().ListLedgers(ctx)
	if err != nil {
		return err
	}

	for _, ledger := range ledgers {
		if err := w.processLedger(ctx, ledger); err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) processLedger(ctx context.Context, ledger string) error {

	store, _, err := w.driver.GetLedgerStore(ctx, ledger, false)
	if err != nil && err != storage.ErrLedgerStoreNotFound {
		return err
	}
	if err == storage.ErrLedgerStoreNotFound {
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

	return nil
}

func (w *Worker) processLogs(ctx context.Context, store storage.LedgerStore, logs ...core.Log) error {
	volumeAggregator := aggregator.Volumes(store)
	var nextTxID *uint64
	lastTx, err := store.GetLastTransaction(ctx)
	if err != nil {
		return err
	}
	if lastTx != nil {
		v := lastTx.ID + 1
		nextTxID = &v
	} else {
		v := uint64(0)
		nextTxID = &v
	}

	for _, log := range logs {
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
			expandedTx.ID = *nextTxID
			*nextTxID = *nextTxID + 1
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
		if err != nil {
			return err
		}
	}

	return nil
}

func NewWorker(config workerConfig, driver storage.Driver, monitor Monitor) *Worker {
	return &Worker{
		stopChan:     make(chan chan struct{}),
		workerConfig: config,
		driver:       driver,
		monitor:      monitor,
	}
}
