package query

import (
	"context"

	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
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

	for _, ledger := range ledgers {
		store, _, err := iq.driver.GetLedgerStore(ctx, ledger, false)
		if err != nil && !storage.IsNotFoundError(err) {
			return err
		}

		if storage.IsNotFoundError(err) {
			continue
		}

		if !store.IsInitialized() {
			continue
		}

		if _, err := initLedger(
			ctx,
			ledger,
			NewDefaultStore(store),
			iq.monitor,
			iq.metricsRegistry,
		); err != nil {
			return err
		}
	}

	return nil
}

func NewInitQuery(driver storage.Driver, monitor monitor.Monitor) *InitQuery {
	return &InitQuery{
		driver:  driver,
		monitor: monitor,
	}
}
