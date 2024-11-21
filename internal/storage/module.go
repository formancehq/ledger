package storage

import (
	"context"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/storage/driver"
	"go.uber.org/fx"
)

func NewFXModule(autoUpgrade bool) fx.Option {
	ret := []fx.Option{
		driver.NewFXModule(),
	}
	if autoUpgrade {
		ret = append(ret,
			fx.Invoke(func(lc fx.Lifecycle, driver *driver.Driver) {
				var (
					upgradeContext        context.Context
					cancelContext         func()
					upgradeStopped        = make(chan struct{})
					minimalVersionReached = make(chan struct{})
				)
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						upgradeContext, cancelContext = context.WithCancel(context.WithoutCancel(ctx))
						go func() {
							defer close(upgradeStopped)

							if err := driver.UpgradeAllBuckets(upgradeContext, minimalVersionReached); err != nil {
								logging.FromContext(ctx).Errorf("failed to upgrade all buckets: %v", err)
							}
						}()

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-minimalVersionReached:
							return nil
						}
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