package runner

import (
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
)

func Module(allowPastTimestamp bool) fx.Option {
	return fx.Options(
		fx.Provide(func(storageDriver storage.Driver, lock lock.Locker, cacheManager *cache.Manager) *Manager {
			return NewManager(storageDriver, lock, cacheManager, allowPastTimestamp)
		}),
	)
}
