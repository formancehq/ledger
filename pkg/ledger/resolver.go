package ledger

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	return ResolveOptionFn(func(r *Resolver) error {
		r.storageFactory = factory
		return nil
	})
}

func WithLocker(locker Locker) ResolveOptionFn {
	return ResolveOptionFn(func(r *Resolver) error {
		r.locker = locker
		return nil
	})
}

var DefaultResolverOptions = []ResolverOption{
	WithStorageFactory(storage.NewDefaultFactory(sqlstorage.NewInMemorySQLiteDriver())),
	WithLocker(NewInMemoryLocker()),
}

type Resolver struct {
	storageFactory    storage.Factory
	locker            Locker
	lock              sync.RWMutex
	initializedStores map[string]struct{}
}

func NewResolver(options ...ResolverOption) *Resolver {
	options = append(DefaultResolverOptions, options...)
	r := &Resolver{
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

	store, err := r.storageFactory.GetStore(name)
	if err != nil {
		return nil, err
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
		err = store.Initialize(ctx)
		if err != nil {
			err = fmt.Errorf("failed to initialize store: %w", err)
			logrus.Debugln(err)
			return nil, err
		}
		r.initializedStores[name] = struct{}{}
	}

ret:
	return NewLedger(name, store, r.locker)
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
			fx.Annotate(NewResolver, fx.ParamTags(ResolverOptionsKey)),
		),
		ProvideResolverOption(WithStorageFactory),
	)
}
