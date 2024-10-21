package storage

import (
	"context"
	"errors"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/storage/driver"
	"go.uber.org/fx"
)

func NewFXModule(autoUpgrade bool) fx.Option {
	ret := []fx.Option{
		driver.NewFXModule(autoUpgrade),
	}
	if autoUpgrade {
		ret = append(ret,
			fx.Invoke(func(lc fx.Lifecycle, driver *driver.Driver) {
				var (
					upgradeContext context.Context
					cancelContext  func()
					upgradeStopped = make(chan struct{})
				)
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						upgradeContext, cancelContext = context.WithCancel(logging.ContextWithLogger(
							context.Background(),
							logging.FromContext(ctx),
						))
						go func() {
							defer close(upgradeStopped)

							if err := driver.UpgradeAllBuckets(upgradeContext); err != nil {
								// Long migrations can be cancelled (app rescheduled for example)
								// before fully terminated, handle this gracefully, don't panic,
								// the next start will try again.
								if errors.Is(err, context.DeadlineExceeded) ||
									errors.Is(err, context.Canceled) {
									return
								}

								panic(err)
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
