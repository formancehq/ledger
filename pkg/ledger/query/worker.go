package query

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
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

	pending             []*ledgerstore.AppendedLog
	writeChannel        chan []*ledgerstore.AppendedLog
	jobs                chan []*ledgerstore.AppendedLog
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
		for _, log := range w.pending {
			//TODO(gfyrag): forward an error
			log.ActiveLog.SetIngested()
		}
		w.pending = nil
		close(stopChan)
	}

	var effectiveSendChannel chan []*ledgerstore.AppendedLog
	for {
		batch := w.pending
		if len(batch) > 0 {
			effectiveSendChannel = w.jobs
			if len(batch) > 1024 {
				batch = batch[:1024]
			}
		} else {
			effectiveSendChannel = nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()

		case stopChan := <-w.stopChan:
			stop(stopChan)
			return nil

		// At this level, the job is writing several models, just accumulate them in a buffer
		case logs := <-w.writeChannel:
			w.pending = append(w.pending, logs...)
			w.metricsRegistry.QueryPendingMessages().Add(ctx, int64(len(w.pending)))

		case effectiveSendChannel <- batch:
			w.pending = w.pending[len(batch):]
			w.metricsRegistry.QueryPendingMessages().Add(ctx, int64(-len(batch)))
		}
	}
}

func (w *Worker) writeLoop(ctx context.Context) {
	closeLogs := func(logs []*ledgerstore.AppendedLog) {
		for _, log := range logs {
			close(log.ActiveLog.Ingested)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stoppedChan:
			close(w.writeLoopTerminated)
			return
		case activeLogs := <-w.jobs:
			logs := make([]core.PersistedLog, len(activeLogs))
			for i, holder := range activeLogs {
				logs[i] = *holder.PersistedLog
			}

			if err := processLogs(ctx, w.ledgerName, w.store, w.monitor, logs...); err != nil {
				panic(err)
			}

			w.metricsRegistry.QueryProcessedLogs().Add(ctx, int64(len(logs)))

			if err := w.store.UpdateNextLogID(ctx, logs[len(logs)-1].ID+1); err != nil {
				panic(err)
			}

			logging.FromContext(ctx).Debugf("Ingested logs until: %d", logs[len(logs)-1].ID)
			closeLogs(activeLogs)
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
	logs ...core.PersistedLog,
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
	logs ...core.PersistedLog,
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

		l:
			for account, volumes := range txVolumeAggregator.PreCommitVolumes {
				for _, volume := range volumes {
					if volume.Output.Cmp(core.Zero) != 0 || volume.Input.Cmp(core.Zero) != 0 {
						continue l
					}
				}
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

			logsData.monitors = append(logsData.monitors, func(ctx context.Context, monitor monitor.Monitor) {
				revertedTx, err := store.GetTransaction(ctx, payload.RevertedTransactionID)
				if err != nil {
					panic(err)
				}
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

func (w *Worker) QueueLog(ctx context.Context, logs ...*ledgerstore.AppendedLog) error {
	select {
	case <-w.stoppedChan:
		return errors.New("worker stopped")
	case w.writeChannel <- logs:
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
		pending:             make([]*ledgerstore.AppendedLog, 0, 1024),
		jobs:                make(chan []*ledgerstore.AppendedLog),
		writeChannel:        make(chan []*ledgerstore.AppendedLog, config.ChanSize),
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
