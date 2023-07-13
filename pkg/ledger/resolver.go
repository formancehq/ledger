package ledger

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/driver"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type option func(r *Resolver)

func WithMessagePublisher(publisher message.Publisher) option {
	return func(r *Resolver) {
		r.publisher = publisher
	}
}

func WithMetricsRegistry(registry metrics.GlobalRegistry) option {
	return func(r *Resolver) {
		r.metricsRegistry = registry
	}
}

func WithCompiler(compiler *command.Compiler) option {
	return func(r *Resolver) {
		r.compiler = compiler
	}
}

func WithLogger(logger logging.Logger) option {
	return func(r *Resolver) {
		r.logger = logger
	}
}

var defaultOptions = []option{
	WithMetricsRegistry(metrics.NewNoOpRegistry()),
	WithCompiler(command.NewCompiler(1024)),
	WithLogger(logging.NewLogrus(logrus.New())),
}

type Resolver struct {
	storageDriver   *driver.Driver
	lock            sync.RWMutex
	metricsRegistry metrics.GlobalRegistry
	//TODO(gfyrag): add a routine to clean old ledger
	ledgers   map[string]*Ledger
	compiler  *command.Compiler
	logger    logging.Logger
	publisher message.Publisher
}

func NewResolver(storageDriver *driver.Driver, options ...option) *Resolver {
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

		logging.FromContext(ctx).Infof("Initialize new ledger")

		ledger, ok = r.ledgers[name]
		if ok {
			return ledger, nil
		}

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

		ledger = New(name, store, r.publisher, r.compiler)
		ledger.Start(logging.ContextWithLogger(context.Background(), r.logger))
		r.ledgers[name] = ledger
		r.metricsRegistry.ActiveLedgers().Add(ctx, +1)
	}

	return ledger, nil
}

func (r *Resolver) CloseLedgers(ctx context.Context) error {
	r.logger.Info("Close all ledgers")
	defer func() {
		r.logger.Info("All ledgers closed")
	}()
	for name, ledger := range r.ledgers {
		r.logger.Infof("Close ledger %s", name)
		ledger.Close(logging.ContextWithLogger(ctx, r.logger.WithField("ledger", name)))
		delete(r.ledgers, name)
	}

	return nil
}
