package ledger

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/pkg/errors"
	"go.uber.org/fx"
)

type Resolver struct {
	storageDriver     storage.Driver
	lock              sync.RWMutex
	initializedStores map[string]struct{}
	locker            lock.Locker
	cacheManager      *cache.Manager
	runnerManager     *runner.Manager
}

func NewResolver(
	storageDriver storage.Driver,
	locker lock.Locker,
	cacheManager *cache.Manager,
	runnerManager *runner.Manager,
) *Resolver {
	return &Resolver{
		storageDriver:     storageDriver,
		cacheManager:      cacheManager,
		runnerManager:     runnerManager,
		initializedStores: map[string]struct{}{},
		locker:            locker,
	}
}

func (r *Resolver) GetLedger(ctx context.Context, name string) (*Ledger, error) {
	store, _, err := r.storageDriver.GetLedgerStore(ctx, name, true)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving ledger store")
	}

	r.lock.RLock()
	_, ok := r.initializedStores[name]
	r.lock.RUnlock()
	if ok {
		goto ret
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok = r.initializedStores[name]; !ok {
		_, err = store.Initialize(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "initializing ledger store")
		}
		r.initializedStores[name] = struct{}{}
	}

ret:
	cache, err := r.cacheManager.ForLedger(ctx, name)
	if err != nil {
		return nil, err
	}
	runner, err := r.runnerManager.ForLedger(ctx, name)
	if err != nil {
		return nil, err
	}

	return New(store, cache, runner, r.locker), nil
}

func Module(allowPastTimestamp bool) fx.Option {
	return fx.Options(
		fx.Provide(NewResolver),
		lock.Module(),
		cache.Module(),
		query.Module(),
		// TODO: Maybe handle this by request ?
		runner.Module(allowPastTimestamp),
	)
}
