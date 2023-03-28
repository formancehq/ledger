package ledger

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/numscript"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/pkg/errors"
)

type Resolver struct {
	storageDriver storage.Driver
	lock          sync.RWMutex
	locker        lock.Locker
	queryWorker   *query.Worker
	//TODO(gfyrag): add a routine to clean old ledger
	ledgers             map[string]*Ledger
	compiler            *numscript.Compiler
	allowPastTimestamps bool
}

func NewResolver(
	storageDriver storage.Driver,
	locker lock.Locker,
	queryWorker *query.Worker,
	allowPastTimestamps bool,
) *Resolver {
	return &Resolver{
		storageDriver:       storageDriver,
		locker:              locker,
		queryWorker:         queryWorker,
		compiler:            numscript.NewCompiler(),
		ledgers:             map[string]*Ledger{},
		allowPastTimestamps: allowPastTimestamps,
	}
}

func (r *Resolver) GetLedger(ctx context.Context, name string) (*Ledger, error) {

	r.lock.RLock()
	ledger, ok := r.ledgers[name]
	r.lock.RUnlock()
	if !ok {
		r.lock.Lock()
		defer r.lock.Unlock()

		store, _, err := r.storageDriver.GetLedgerStore(ctx, name, true)
		if err != nil {
			return nil, errors.Wrap(err, "retrieving ledger store")
		}
		if !store.IsInitialized() {
			if _, err := store.Initialize(ctx); err != nil {
				return nil, err
			}
		}

		cache := cache.New(store)
		runner, err := runner.New(store, r.locker, cache, r.compiler, r.allowPastTimestamps)
		if err != nil {
			return nil, err
		}

		ledger = New(store, cache, runner, r.locker, r.queryWorker)
		r.ledgers[name] = ledger
	}

	return ledger, nil
}
