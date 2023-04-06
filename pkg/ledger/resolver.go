package ledger

import (
	"context"
	"sync"
	"time"

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

type option func(r *Resolver)

func WithMonitor(monitor monitor.Monitor) option {
	return func(r *Resolver) {
		r.monitor = monitor
	}
}

func WithAllowPastTimestamps() option {
	return func(r *Resolver) {
		r.allowPastTimestamps = true
	}
}

func WithMetricsRegistry(registry metrics.GlobalMetricsRegistry) option {
	return func(r *Resolver) {
		r.metricsRegistry = registry
	}
}

func WithCacheEvictionRetainDelay(t time.Duration) option {
	return func(r *Resolver) {
		r.cacheEvictionRetainDelay = t
	}
}

func WithCacheEvictionPeriod(t time.Duration) option {
	return func(r *Resolver) {
		r.cacheEvictionPeriod = t
	}
}

var defaultOptions = []option{
	WithMetricsRegistry(metrics.NewNoOpMetricsRegistry()),
	WithMonitor(monitor.NewNoOpMonitor()),
}

type Resolver struct {
	storageDriver   storage.Driver
	monitor         monitor.Monitor
	lock            sync.RWMutex
	metricsRegistry metrics.GlobalMetricsRegistry
	//TODO(gfyrag): add a routine to clean old ledger
	ledgers                                       map[string]*Ledger
	compiler                                      *numscript.Compiler
	allowPastTimestamps                           bool
	cacheEvictionPeriod, cacheEvictionRetainDelay time.Duration
}

func NewResolver(storageDriver storage.Driver, options ...option) *Resolver {
	r := &Resolver{
		storageDriver: storageDriver,
		compiler:      numscript.NewCompiler(),
		ledgers:       map[string]*Ledger{},
	}
	for _, opt := range append(defaultOptions, options...) {
		opt(r)
	}

	return r
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

		cacheOptions := []cache.Option{
			cache.WithMetricsRegistry(metricsRegistry),
		}
		if r.cacheEvictionPeriod != 0 {
			cacheOptions = append(cacheOptions, cache.WithEvictionPeriod(r.cacheEvictionPeriod))
		}
		if r.cacheEvictionRetainDelay != 0 {
			cacheOptions = append(cacheOptions, cache.WithRetainDelay(r.cacheEvictionRetainDelay))
		}

		cache := cache.New(store, cacheOptions...)
		go func() {
			if err := cache.Run(context.Background()); err != nil {
				panic(err)
			}
		}()

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
