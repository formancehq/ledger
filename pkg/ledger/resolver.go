package ledger

import (
	"context"
	"sync"

	"github.com/dgraph-io/ristretto"
	"github.com/formancehq/ledger/pkg/storage"
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

func WithMonitor(monitor Monitor) ResolveOptionFn {
	return func(r *Resolver) error {
		r.monitor = monitor
		return nil
	}
}

var DefaultResolverOptions = []ResolverOption{
	WithMonitor(&noOpMonitor{}),
}

type Resolver struct {
	storageDriver     storage.Driver
	lock              sync.RWMutex
	initializedStores map[string]struct{}
	monitor           Monitor
	ledgerOptions     []LedgerOption
	cache             *ristretto.Cache
	locker            Locker
}

func NewResolver(
	storageDriver storage.Driver,
	ledgerOptions []LedgerOption,
	cacheBytesCapacity, cacheMaxNumKeys int64,
	locker Locker,
	options ...ResolverOption,
) *Resolver {
	options = append(DefaultResolverOptions, options...)
	r := &Resolver{
		storageDriver:     storageDriver,
		initializedStores: map[string]struct{}{},
		cache:             NewCache(cacheBytesCapacity, cacheMaxNumKeys, false),
		locker:            locker,
	}
	for _, opt := range options {
		if err := opt.apply(r); err != nil {
			panic(errors.Wrap(err, "applying option on resolver"))
		}
	}
	r.ledgerOptions = ledgerOptions

	return r
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
		return NewLedger(store, r.monitor, r.cache, r.ledgerOptions...)
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

	return NewLedger(store, r.monitor, r.cache, append(r.ledgerOptions, WithLocker(r.locker))...)
}

func (r *Resolver) Close() {
	r.cache.Close()
}

const ResolverOptionsKey = `group:"_ledgerResolverOptions"`
const ResolverLedgerOptionsKey = `name:"_ledgerResolverLedgerOptions"`

func ProvideResolverOption(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(ResolverOptionsKey), fx.As(new(ResolverOption))),
	)
}

func ResolveModule(cacheBytesCapacity, cacheMaxNumKeys int64) fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(func(storageFactory storage.Driver, ledgerOptions []LedgerOption, locker Locker, options ...ResolverOption) *Resolver {
				return NewResolver(storageFactory, ledgerOptions, cacheBytesCapacity, cacheMaxNumKeys, locker, options...)
			}, fx.ParamTags("", ResolverLedgerOptionsKey, "", ResolverOptionsKey)),
		),
		fx.Provide(func() Locker {
			return NewInMemoryLocker()
		}),
		fx.Invoke(func(lc fx.Lifecycle, r *Resolver) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					r.Close()
					return nil
				},
			})
		}),
	)
}
