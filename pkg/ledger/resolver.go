package ledger

import (
	"context"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
	"go.uber.org/fx"
	"sync"
)

type ResolverOption interface {
	apply(r *Resolver) error
}
type ResolveOptionFn func(r *Resolver) error

func (fn ResolveOptionFn) apply(r *Resolver) error {
	return fn(r)
}

func WithStorageFactory(factory storage.Factory) ResolveOptionFn {
	return func(r *Resolver) error {
		r.storageFactory = factory
		return nil
	}
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
	storageFactory    storage.Factory
	locker            Locker
	lock              sync.RWMutex
	initializedStores map[string]struct{}
	monitor           Monitor
}

func NewResolver(storageFactory storage.Factory, options ...ResolverOption) *Resolver {
	options = append(DefaultResolverOptions, options...)
	r := &Resolver{
		storageFactory:    storageFactory,
		initializedStores: map[string]struct{}{},
	}
	for _, opt := range options {
		err := opt.apply(r)
		if err != nil {
			panic(errors.Wrap(err, "applying option on resolver"))
		}
	}

	return r
}

func (r *Resolver) GetLedger(ctx context.Context, name string) (*Ledger, error) {

	store, err := r.storageFactory.GetStore(ctx, name)
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

	_, ok = r.initializedStores[name]
	if !ok {
		_, err = store.Initialize(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "initializing ledger store")
		}
		r.initializedStores[name] = struct{}{}
	}

ret:
	return NewLedger(name, store, r.locker, r.monitor)
}

const ResolverOptionsKey = `group:"_ledgerResolverOptions"`

func ProvideResolverOption(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(ResolverOptionsKey), fx.As(new(ResolverOption))),
	)
}

func ResolveModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(NewResolver, fx.ParamTags("", ResolverOptionsKey)),
		),
		ProvideResolverOption(WithStorageFactory),
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
