package ledger

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

type option func(r *Resolver)

func WithMonitor(monitor monitor.Monitor) option {
	return func(r *Resolver) {
		r.monitor = monitor
	}
}

func WithMetricsRegistry(registry metrics.GlobalMetricsRegistry) option {
	return func(r *Resolver) {
		r.metricsRegistry = registry
	}
}

func WithCompiler(compiler *command.Compiler) option {
	return func(r *Resolver) {
		r.compiler = compiler
	}
}

var defaultOptions = []option{
	WithMetricsRegistry(metrics.NewNoOpMetricsRegistry()),
	WithMonitor(monitor.NewNoOpMonitor()),
	WithCompiler(command.NewCompiler(1024)),
}

type Resolver struct {
	storageDriver   *storage.Driver
	monitor         monitor.Monitor
	lock            sync.RWMutex
	metricsRegistry metrics.GlobalMetricsRegistry
	//TODO(gfyrag): add a routine to clean old ledger
	ledgers  map[string]*Ledger
	compiler *command.Compiler
}

func NewResolver(storageDriver *storage.Driver, options ...option) *Resolver {
	r := &Resolver{
		storageDriver: storageDriver,
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

		exists, err := r.storageDriver.GetSystemStore().Exists(ctx, name)
		if err != nil {
			return nil, err
		}

		var store *ledgerstore.Store
		if !exists {
			store, err = r.storageDriver.CreateLedgerStore(ctx, name)
		} else {
			store, err = r.storageDriver.GetLedgerStore(ctx, name)
		}
		if err != nil {
			return nil, err
		}

		if !store.IsInitialized() {
			if _, err := store.Migrate(ctx); err != nil {
				return nil, errors.Wrap(err, "initializing ledger store")
			}
		}

		backgroundContext := logging.ContextWithLogger(
			context.Background(),
			logging.FromContext(ctx),
		)
		runOrPanic := func(task func(context.Context) error) {
			go func() {
				if err := task(backgroundContext); err != nil {
					panic(err)
				}
			}()
		}

		locker := command.NewDefaultLocker(name)
		runOrPanic(locker.Run)

		metricsRegistry, err := metrics.RegisterPerLedgerMetricsRegistry(name)
		if err != nil {
			return nil, errors.Wrap(err, "registering metrics")
		}

		queryWorker := query.NewWorker(query.DefaultWorkerConfig, query.NewDefaultStore(store), name, r.monitor, metricsRegistry)
		runOrPanic(queryWorker.Run)

		ledger = New(store, locker, queryWorker, r.compiler, metricsRegistry)
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
