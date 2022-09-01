package ledger

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/fx"
)

type ResolverOption interface {
	apply(r *Resolver) error
}
type ResolveOptionFn func(r *Resolver) error

func (fn ResolveOptionFn) apply(r *Resolver) error {
	return fn(r)
}

func WithLocker(locker Locker) ResolveOptionFn {
	return func(r *Resolver) error {
		r.locker = locker
		return nil
	}
}

func WithMonitor(monitor Monitor) ResolveOptionFn {
	return func(r *Resolver) error {
		r.monitor = monitor
		return nil
	}
}

var DefaultResolverOptions = []ResolverOption{
	WithLocker(NewInMemoryLocker()),
	WithMonitor(&noOpMonitor{}),
}

type Resolver struct {
	storageDriver     StorageDriver
	locker            Locker
	lock              sync.RWMutex
	initializedStores map[string]struct{}
	monitor           Monitor
	ledgerOptions     []LedgerOption
}

func NewResolver(
	storageFactory StorageDriver,
	ledgerOptions []LedgerOption,
	options ...ResolverOption,
) *Resolver {
	options = append(DefaultResolverOptions, options...)
	r := &Resolver{
		storageDriver:     storageFactory,
		initializedStores: map[string]struct{}{},
	}
	for _, opt := range options {
		err := opt.apply(r)
		if err != nil {
			panic(errors.Wrap(err, "applying option on resolver"))
		}
	}
	r.ledgerOptions = ledgerOptions

	return r
}

func (r *Resolver) GetLedger(ctx context.Context, name string) (*Ledger, error) {
	store, _, err := r.storageDriver.GetStore(ctx, name, true)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving ledger store")
	}

	r.lock.RLock()
	_, ok := r.initializedStores[name]
	r.lock.RUnlock()
	if ok {
		return NewLedger(store, r.locker, r.monitor, r.ledgerOptions...)
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

	return NewLedger(store, r.locker, r.monitor, r.ledgerOptions...)
}

const ResolverOptionsKey = `group:"_ledgerResolverOptions"`
const ResolverLedgerOptionsKey = `name:"_ledgerResolverLedgerOptions"`

func ProvideResolverOption(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(ResolverOptionsKey), fx.As(new(ResolverOption))),
	)
}

func ResolveModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(NewResolver, fx.ParamTags("", ResolverLedgerOptionsKey, ResolverOptionsKey)),
		),
	)
}

func MemoryLockModule() fx.Option {
	return fx.Options(
		ProvideResolverOption(func() ResolverOption {
			return WithLocker(NewInMemoryLocker())
		}),
	)
}

func NoLockModule() fx.Option {
	return fx.Options(
		ProvideResolverOption(func() ResolverOption {
			return WithLocker(NoOpLocker)
		}),
	)
}
