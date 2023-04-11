package query

import (
	"context"

	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
	"golang.org/x/sync/errgroup"
)

func QueryInitModule() fx.Option {
	return fx.Options(
		fx.Provide(NewInitQuery),
		fx.Invoke(func(lc fx.Lifecycle, initQuery *InitQuery) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return initQuery.initLedgers(ctx)
				},
			})
		}),
	)
}

type InitQuery struct {
	driver          storage.Driver
	monitor         monitor.Monitor
	metricsRegistry metrics.PerLedgerMetricsRegistry
}

func (iq InitQuery) initLedgers(ctx context.Context) error {
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

func NewInitQuery(driver storage.Driver, monitor monitor.Monitor) *InitQuery {
	return &InitQuery{
		driver:  driver,
		monitor: monitor,
	}
}
