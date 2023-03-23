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
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

type Resolver struct {
	storageDriver   storage.Driver
	monitor         monitor.Monitor
	lock            sync.RWMutex
	metricsRegistry metrics.GlobalMetricsRegistry
	//TODO(gfyrag): add a routine to clean old ledger
	ledgers             map[string]*Ledger
	compiler            *numscript.Compiler
	allowPastTimestamps bool
}

func NewResolver(storageDriver storage.Driver, monitor monitor.Monitor, allowPastTimestamps bool, metricsRegistry metrics.GlobalMetricsRegistry) *Resolver {
	return &Resolver{
		storageDriver:       storageDriver,
		monitor:             monitor,
		compiler:            numscript.NewCompiler(),
		ledgers:             map[string]*Ledger{},
		allowPastTimestamps: allowPastTimestamps,
		metricsRegistry:     metricsRegistry,
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

		locker := lock.New(name)
		go func() {
			if err := locker.Run(context.Background()); err != nil {
				panic(err)
			}
		}()

		metricsRegistry, err := metrics.RegisterPerLedgerMetricsRegistry(name)
		if err != nil {
			return nil, errors.Wrap(err, "registering metrics")
		}

		cache := cache.New(store, metricsRegistry)
		runner, err := runner.New(store, locker, cache, r.compiler, name, r.allowPastTimestamps)
		if err != nil {
			return nil, errors.Wrap(err, "creating ledger runner")
		}

		queryWorker := query.NewWorker(query.DefaultWorkerConfig, query.NewDefaultStore(store), name, r.monitor, metricsRegistry)

		go func() {
			if err := queryWorker.Run(logging.ContextWithLogger(
				context.Background(),
				logging.FromContext(ctx),
			)); err != nil {
				panic(err)
			}
		}()

		ledger = New(store, cache, runner, locker, queryWorker, metricsRegistry)
		r.ledgers[name] = ledger
		r.metricsRegistry.ActiveLedgers().Add(ctx, +1)
	}

	return ledger, nil
}

func (r *Resolver) CloseLedgers(ctx context.Context) error {
	for name, ledger := range r.ledgers {
		if err := ledger.Close(ctx); err != nil {
			return err
		}
		delete(r.ledgers, name)
	}

	return nil
}
