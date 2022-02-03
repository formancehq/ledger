package ledger

import (
	"context"
	"sync"
)

type Unlock func(ctx context.Context)

type Locker interface {
	Lock(ctx context.Context, name string) (Unlock, error)
}

type InMemoryLocker struct {
	globalLock sync.RWMutex
	locks      map[string]*sync.Mutex
}

func (d *InMemoryLocker) Lock(ctx context.Context, ledger string) (Unlock, error) {
	d.globalLock.RLock()
	lock, ok := d.locks[ledger]
	d.globalLock.RUnlock()
	if ok {
		goto ret
	}

	d.globalLock.Lock()
	lock, ok = d.locks[ledger] // Double check, the lock can have been acquired by another go routing between RUnlock and Lock
	if !ok {
		lock = &sync.Mutex{}
		d.locks[ledger] = lock
	}
	d.globalLock.Unlock()
ret:
	lock.Lock()
	return func(ctx context.Context) {
		lock.Unlock()
	}, nil
}

func NewInMemoryLocker() *InMemoryLocker {
	return &InMemoryLocker{
		locks: map[string]*sync.Mutex{},
	}
}
