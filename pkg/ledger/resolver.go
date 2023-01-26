package ledger

import (
	"context"
	"sync"

	"github.com/dgraph-io/ristretto"
	"github.com/numary/ledger/pkg/storage"
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
	storageDriver     storage.Driver[Store]
	lock              sync.RWMutex
	initializedStores map[string]struct{}
	monitor           Monitor
	ledgerOptions     []LedgerOption
	cache             *ristretto.Cache
}

func NewResolver(
	storageFactory storage.Driver[Store],
	ledgerOptions []LedgerOption,
	options ...ResolverOption,
) *Resolver {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,    // number of keys to track frequency of (10M).
		MaxCost:     1 << 8, // maximum cost of cache (100MB).
		BufferItems: 64,     // number of keys per Get buffer.
	})
	if err != nil {
		panic(errors.Wrap(err, "creating ledger cache"))
	}

	options = append(DefaultResolverOptions, options...)
	r := &Resolver{
		storageDriver:     storageFactory,
		initializedStores: map[string]struct{}{},
		cache:             cache,
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

	return NewLedger(store, r.monitor, r.cache, r.ledgerOptions...)
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
