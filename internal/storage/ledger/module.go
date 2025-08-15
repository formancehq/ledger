package ledger

import (
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/workers/lockmonitor"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

func NewModule() fx.Option {
	return fx.Options(
		fx.Invoke(func(db *bun.DB) {
			db.Dialect().Tables().Register(
				&ledger.Transaction{},
				&ledger.Log{},
				&ledger.Account{},
				&ledger.Move{},
			)
		}),
		fx.Provide(func(params struct {
			fx.In

			DB             *bun.DB
			TracerProvider trace.TracerProvider `optional:"true"`
			MeterProvider  metric.MeterProvider `optional:"true"`
		}) Factory {
			options := make([]Option, 0)
			if params.TracerProvider != nil {
				options = append(options, WithTracer(params.TracerProvider.Tracer("store")))
			}
			if params.MeterProvider != nil {
				options = append(options, WithMeter(params.MeterProvider.Meter("store")))
			}
			return NewFactory(params.DB, options...)
		}),
		fx.Provide(fx.Annotate(func(db *bun.DB, meterProvider metric.MeterProvider) (lockmonitor.Option, error) {
			histogram, err := meterProvider.Meter("lockmonitor").
				Int64Histogram("lockmonitor.accounts_volumes_locks", metric.WithDescription("Accounts volumes histogram"))
			if err != nil {
				return nil, err
			}

			return lockmonitor.WithMonitors(
				NewAccountsVolumesMonitor(newOtlpRecorder(histogram)),
			), nil
		}, fx.ResultTags(`group:"lockmonitor.options"`))),
	)
}
