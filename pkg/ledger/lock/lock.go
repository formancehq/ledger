package lock

import (
	"context"
	"sync"
)

type Unlock func(ctx context.Context)

type Locker interface {
	Lock(ctx context.Context, ledger string, accounts ...string) (Unlock, error)
}
type LockerFn func(ctx context.Context, ledger string, accounts ...string) (Unlock, error)

func (fn LockerFn) Lock(ctx context.Context, ledger string, accounts ...string) (Unlock, error) {
	return fn(ctx, ledger, accounts...)
}

var NoOpLocker = LockerFn(func(ctx context.Context, ledger string, accounts ...string) (Unlock, error) {
	return func(ctx context.Context) {}, nil
})

type InMemory struct {
	globalLock sync.RWMutex
	locks      map[string]*sync.Mutex
}

func (d *InMemory) Lock(ctx context.Context, ledger string, accounts ...string) (Unlock, error) {
	d.globalLock.RLock()
	lock, ok := d.locks[ledger]
	d.globalLock.RUnlock()
	if !ok {
		d.globalLock.Lock()
		lock, ok = d.locks[ledger] // Double check, the lock can have been acquired by another go routing between RUnlock and Lock
		if !ok {
			lock = &sync.Mutex{}
			d.locks[ledger] = lock
		}
		d.globalLock.Unlock()
	}

	unlocked := false
	lock.Lock()
	return func(ctx context.Context) {
		if unlocked {
			return
		}
		lock.Unlock()
		unlocked = true
	}, nil
}

func NewInMemory() *InMemory {
	return &InMemory{
		locks: map[string]*sync.Mutex{},
	}
}
