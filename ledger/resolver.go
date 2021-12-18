package ledger

import (
	"context"
	"github.com/numary/ledger/storage"
	"github.com/pkg/errors"
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
	WithStorageFactory(&storage.BuiltInFactory{Driver: "sqlite"}),
	WithLocker(NewInMemoryLocker()),
}

type Resolver struct {
	storageFactory storage.Factory
	locker         Locker
}

func NewResolver(options ...ResolverOption) *Resolver {
	options = append(DefaultResolverOptions, options...)
	r := &Resolver{}
	for _, opt := range options {
		err := opt.apply(r)
		if err != nil {
			panic(errors.Wrap(err, "applying option on resolver"))
		}
	}

	return r
}

func (r *Resolver) GetLedger(ctx context.Context, name string) (*Ledger, error) {
	return NewLedger(ctx, name, r.storageFactory, r.locker)
}
