package query

import (
	"context"

	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func initLedger(
	ctx context.Context,
	ledgerName string,
	store Store,
	monitor monitor.Monitor,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) (uint64, error) {
	if !store.IsInitialized() {
		return 0, nil
	}

	nextLogIDToProcess, err := store.GetNextLogID(ctx)
	if err != nil && !storage.IsNotFoundError(err) {
		return 0, errorsutil.NewError(ErrStorage,
			errors.Wrap(err, "reading last log"))
	}

	logs, err := store.ReadLogsStartingFromID(ctx, nextLogIDToProcess)
	if err != nil {
		return 0, errorsutil.NewError(ErrStorage,
			errors.Wrap(err, "reading logs since last ID"))
	}

	if len(logs) == 0 {
		return 0, nil
	}

	if err := processLogs(ctx, ledgerName, store, monitor, logs...); err != nil {
		return 0, errors.Wrap(err, "processing logs")
	}

	metricsRegistry.QueryProcessedLogs().Add(ctx, int64(len(logs)))

	if err := store.UpdateNextLogID(ctx, logs[len(logs)-1].ID+1); err != nil {
		return 0, errorsutil.NewError(ErrStorage,
			errors.Wrap(err, "updating last read log"))
	}
	lastProcessedLogID := logs[len(logs)-1].ID

	return lastProcessedLogID, nil
}

type InitLedger struct {
	driver          storage.Driver
	monitor         monitor.Monitor
	metricsRegistry metrics.PerLedgerMetricsRegistry
}

func (iq InitLedger) initLedgers(ctx context.Context) error {
	ledgers, err := iq.driver.GetSystemStore().ListLedgers(ctx)
	if err != nil {
		return err
	}

	eg, ctxGroup := errgroup.WithContext(ctx)
	for _, ledger := range ledgers {
		_ledger := ledger
		eg.Go(func() error {
			store, _, err := iq.driver.GetLedgerStore(ctxGroup, _ledger, false)
			if err != nil && !storage.IsNotFoundError(err) {
				return err
			}

			if storage.IsNotFoundError(err) {
				return nil
			}

			if !store.IsInitialized() {
				return nil
			}

			if _, err := initLedger(
				ctxGroup,
				_ledger,
				NewDefaultStore(store),
				iq.monitor,
				iq.metricsRegistry,
			); err != nil {
				return err
			}

			return nil
		})
	}

	return eg.Wait()
}

func NewInitLedgers(driver storage.Driver, monitor monitor.Monitor) *InitLedger {
	return &InitLedger{
		driver:  driver,
		monitor: monitor,
	}
}
