package storage

import (
	"context"
	"errors"
	"github.com/formancehq/go-libs/v2/health"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/tracing"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const HealthCheckName = `storage-driver-up-to-date`

type ModuleConfig struct {
	AutoUpgrade bool
}

func NewFXModule(config ModuleConfig) fx.Option {
	ret := []fx.Option{
		driver.NewFXModule(),
		health.ProvideHealthCheck(func(driver *driver.Driver, tracer trace.TracerProvider) health.NamedCheck {
			hasReachedMinimalVersion := false
			return health.NewNamedCheck(HealthCheckName, health.CheckFn(func(ctx context.Context) error {
				_, err := tracing.Trace(ctx, tracer.Tracer("HealthCheck"), "HealthCheckStorage", tracing.NoResult(func(ctx context.Context) error {
					if hasReachedMinimalVersion {
						return nil
					}
					var err error
					hasReachedMinimalVersion, err = driver.HasReachMinimalVersion(ctx)
					if err != nil {
						return err
					}
					if !hasReachedMinimalVersion {
						return errors.New("storage driver is not up to date")
					}
					return nil
				}))
				return err
			}))
		}),
	}
	if config.AutoUpgrade {
		ret = append(ret,
			fx.Invoke(func(lc fx.Lifecycle, driver *driver.Driver) {
				var (
					upgradeContext context.Context
					cancelContext  func()
					upgradeStopped = make(chan struct{})
				)
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						upgradeContext, cancelContext = context.WithCancel(context.WithoutCancel(ctx))
						go func() {
							defer close(upgradeStopped)

							if err := driver.UpgradeAllBuckets(upgradeContext); err != nil {
								logging.FromContext(ctx).Errorf("failed to upgrade all buckets: %v", err)
							}
						}()

						return nil
					},
					OnStop: func(ctx context.Context) error {
						cancelContext()
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-upgradeStopped:
							return nil
						}
					},
				})
			}),
		)
	}
	return fx.Options(ret...)
}
