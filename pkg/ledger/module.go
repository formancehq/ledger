package ledger

import (
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
)

func Module(allowPastTimestamp bool) fx.Option {
	return fx.Options(
		lock.Module(),
		fx.Provide(func(
			storageDriver storage.Driver,
			monitor monitor.Monitor,
			locker lock.Locker,
		) *Resolver {
			return NewResolver(storageDriver, monitor, locker, allowPastTimestamp)
		}),
		fx.Provide(fx.Annotate(monitor.NewNoOpMonitor, fx.As(new(monitor.Monitor)))),
	)
}
