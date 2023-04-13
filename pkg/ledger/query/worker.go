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

	pending             []*core.LogHolder
	writeChannel        chan *core.LogHolder
	jobs                chan []*core.LogHolder
	releasedJob         chan struct{}
	stopChan            chan chan struct{}
	stoppedChan         chan struct{}
	writeLoopTerminated chan struct{}
	readyChan           chan struct{}

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

	close(w.readyChan)

	go w.writeLoop(ctx)

	stop := func(stopChan chan struct{}) {
		close(w.stoppedChan)
		select {
		case <-ctx.Done():
		case <-w.writeLoopTerminated:
		}
		close(stopChan)
	}

l:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case stopChan := <-w.stopChan:
			stop(stopChan)
			return nil

		// At this level, the job is writting some models, just accumulate models in a buffer
		case wl := <-w.writeChannel:
			w.pending = append(w.pending, wl)
			w.metricsRegistry.QueryPendingMessages().Add(ctx, int64(len(w.pending)))

		case <-w.releasedJob:
			// There, write model job is not running, and we have pending models
			// So we can try to send pending to the job channel
			if len(w.pending) > 0 {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()

					case stopChan := <-w.stopChan:
						stop(stopChan)
						return nil

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
				stop(stopChan)
				return nil
			// There, the job is waiting, and we don't have any pending models to write
			// so, wait for new models to write and send them directly to the job channel
			// We can not return to the main loop as w.releasedJob will be continuously notified by the job routine
			case mh := <-w.writeChannel:
				select {
				case <-ctx.Done():
					return ctx.Err()
				case stopChan := <-w.stopChan:
					stop(stopChan)
					return nil
				case w.jobs <- []*core.LogHolder{mh}:
				}
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
		case <-w.stoppedChan:
			close(w.writeLoopTerminated)
			return
		case w.releasedJob <- struct{}{}:
		case modelsHolder := <-w.jobs:
			logs := make([]core.Log, len(modelsHolder))
			for i, holder := range modelsHolder {
				logs[i] = *holder.Log
			}

			if err := processLogs(ctx, w.ledgerName, w.store, w.monitor, logs...); err != nil {
				panic(err)
			}

			w.metricsRegistry.QueryProcessedLogs().Add(ctx, int64(len(logs)))

			if err := w.store.UpdateNextLogID(ctx, logs[len(logs)-1].ID+1); err != nil {
				panic(err)
			}

			logging.FromContext(ctx).Debugf("Ingested logs until: %d", logs[len(logs)-1].ID)
			closeLogs(modelsHolder)
		}
	}
}

func (w *Worker) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.stopChan <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
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
	transactionsToUpdate := make(map[uint64]metadata.Metadata)

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

func (w *Worker) QueueLog(ctx context.Context, log *core.LogHolder) error {
	select {
	case <-w.stoppedChan:
		return errors.New("worker stopped")
	case w.writeChannel <- log:
		w.metricsRegistry.QueryInboundLogs().Add(ctx, 1)
		return nil
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
		pending:             make([]*core.LogHolder, 0),
		jobs:                make(chan []*core.LogHolder),
		releasedJob:         make(chan struct{}, 1),
		writeChannel:        make(chan *core.LogHolder, config.ChanSize),
		stopChan:            make(chan chan struct{}),
		readyChan:           make(chan struct{}),
		stoppedChan:         make(chan struct{}),
		writeLoopTerminated: make(chan struct{}),
		WorkerConfig:        config,
		store:               store,
		monitor:             monitor,
		ledgerName:          ledgerName,
		metricsRegistry:     metricsRegistry,
	}
}
