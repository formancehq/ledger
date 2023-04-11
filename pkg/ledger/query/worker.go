package query

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

var (
	DefaultWorkerConfig = WorkerConfig{
		ChanSize: 1024,
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
	readyChan    chan struct{}

	store           Store
	monitor         monitor.Monitor
	metricsRegistry metrics.PerLedgerMetricsRegistry

	ledgerName string
}

func (w *Worker) Ready() chan struct{} {
	return w.readyChan
}

func (w *Worker) Run(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Start CQRS worker")

	w.ctx = ctx

	close(w.readyChan)

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

			if err := processLogs(w.ctx, w.ledgerName, w.store, w.monitor, logs...); err != nil {
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

			w.metricsRegistry.QueryProcessedLogs().Add(w.ctx, int64(len(logs)))

			if err := w.store.UpdateNextLogID(w.ctx, logs[len(logs)-1].ID+1); err != nil {
				logging.FromContext(w.ctx).Errorf("CQRS worker error: %s", err)
				closeLogs(modelsHolder)

				// TODO(polo/gfyrag): add idempotency tests
				// Return the error to restart the worker
				w.errorChan <- err
				return
			}

			logging.FromContext(ctx).Infof("Ingested logs until: %d", logs[len(logs)-1].ID)
			closeLogs(modelsHolder)
		}
	}
}

func (w *Worker) run() error {
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
			w.pending = append(w.pending, wl)
			w.metricsRegistry.QueryPendingMessages().Add(w.ctx, int64(len(w.pending)))

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

func processLogs(
	ctx context.Context,
	ledgerName string,
	store Store,
	m monitor.Monitor,
	logs ...core.Log,
) error {
	logsData, err := buildData(ctx, ledgerName, store, logs...)
	if err != nil {
		return errors.Wrap(err, "building data")
	}

	if err := store.RunInTransaction(ctx, func(ctx context.Context, tx Store) error {
		if len(logsData.ensureAccountsExist) > 0 {
			if err := tx.EnsureAccountsExist(ctx, logsData.ensureAccountsExist); err != nil {
				return errors.Wrap(err, "ensuring accounts exist")
			}
		}
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

		if len(logsData.volumesToUpdate) > 0 {
			return tx.UpdateVolumes(ctx, logsData.volumesToUpdate...)
		}

		return nil
	}); err != nil {
		return errorsutil.NewError(ErrStorage, err)
	}

	if m != nil {
		for _, monitor := range logsData.monitors {
			monitor(ctx, m)
		}
	}

	return nil
}

func buildData(
	ctx context.Context,
	ledgerName string,
	store Store,
	logs ...core.Log,
) (*logsData, error) {
	logsData := &logsData{}

	volumeAggregator := aggregator.Volumes(store)
	accountsToUpdate := make(map[string]metadata.Metadata)
	transactionsToUpdate := make(map[string]metadata.Metadata)

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
				monitor.CommittedTransactions(ctx, ledgerName, expandedTx)
				for account, metadata := range payload.AccountMetadata {
					monitor.SavedMetadata(ctx, ledgerName, core.MetaTargetTypeAccount, account, metadata)
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
				id := setMetadata.TargetID.(string)
				if m, ok := transactionsToUpdate[id]; !ok {
					transactionsToUpdate[id] = setMetadata.Metadata
				} else {
					for k, v := range setMetadata.Metadata {
						m[k] = v
					}
				}
			}

			logsData.monitors = append(logsData.monitors, func(ctx context.Context, monitor monitor.Monitor) {
				monitor.SavedMetadata(ctx, ledgerName, setMetadata.TargetType, fmt.Sprint(setMetadata.TargetID), setMetadata.Metadata)
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
				return nil, errorsutil.NewError(ErrStorage, errors.Wrap(err, "aggregating volumes"))
			}

			expandedTx := core.ExpandedTransaction{
				Transaction:       payload.RevertTransaction,
				PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes,
				PostCommitVolumes: txVolumeAggregator.PostCommitVolumes,
			}
			logsData.transactionsToInsert = append(logsData.transactionsToInsert, expandedTx)

			revertedTx, err := store.GetTransaction(ctx, payload.RevertedTransactionID)
			if err != nil {
				return nil, errorsutil.NewError(ErrStorage, err)
			}

			logsData.monitors = append(logsData.monitors, func(ctx context.Context, monitor monitor.Monitor) {
				monitor.RevertedTransaction(ctx, ledgerName, revertedTx, &expandedTx)
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

func (w *Worker) QueueLog(log *core.LogHolder) {
	select {
	case <-w.ctx.Done():
	case w.writeChannel <- log:
		w.metricsRegistry.QueryInboundLogs().Add(w.ctx, 1)
	}
}

func NewWorker(
	config WorkerConfig,
	store Store,
	ledgerName string,
	monitor monitor.Monitor,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) *Worker {
	return &Worker{
		pending:         make([]*core.LogHolder, 0),
		jobs:            make(chan []*core.LogHolder),
		releasedJob:     make(chan struct{}, 1),
		writeChannel:    make(chan *core.LogHolder, config.ChanSize),
		errorChan:       make(chan error, 1),
		stopChan:        make(chan chan struct{}),
		readyChan:       make(chan struct{}),
		WorkerConfig:    config,
		store:           store,
		monitor:         monitor,
		ledgerName:      ledgerName,
		metricsRegistry: metricsRegistry,
	}
}
