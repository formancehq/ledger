package ledger

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/ledger/numscript"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

type Resolver struct {
	storageDriver storage.Driver
	monitor       monitor.Monitor
	lock          sync.RWMutex
	locker        lock.Locker
	//TODO(gfyrag): add a routine to clean old ledger
	ledgers             map[string]*Ledger
	compiler            *numscript.Compiler
	allowPastTimestamps bool
}

func NewResolver(
	storageDriver storage.Driver,
	monitor monitor.Monitor,
	locker lock.Locker,
	allowPastTimestamps bool,
) *Resolver {
	return &Resolver{
		storageDriver:       storageDriver,
		monitor:             monitor,
		locker:              locker,
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
				return nil, errors.Wrap(err, "initializing ledger store")
			}
		}

		cache := cache.New(store)
		runner, err := runner.New(store, r.locker, cache, r.compiler, name, r.allowPastTimestamps)
		if err != nil {
			return nil, errors.Wrap(err, "creating ledger runner")
		}

		queryWorker := query.NewWorker(query.DefaultWorkerConfig, query.NewDefaultStore(store), name, r.monitor)

		go func() {
			if err := queryWorker.Run(logging.ContextWithLogger(
				context.Background(),
				logging.FromContext(ctx),
			)); err != nil {
				panic(err)
			}
		}()

		ledger = New(store, cache, runner, r.locker, queryWorker)
		r.ledgers[name] = ledger
	}

	return ledger, nil
}
