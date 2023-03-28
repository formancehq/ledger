package ledger

import (
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
)

func Module(allowPastTimestamp bool) fx.Option {
	return fx.Options(
		lock.Module(),
		fx.Provide(func(
			storageDriver storage.Driver,
			locker lock.Locker,
			queryWorker *query.Worker,
		) *Resolver {
			return NewResolver(storageDriver, locker, queryWorker, allowPastTimestamp)
		}),
		query.Module(),
	)
}
