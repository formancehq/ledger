package ledger

import "sync"

type Unlock func()

type Locker interface {
	Lock(name string) (Unlock, error)
}

type InMemoryLocker struct {
	globalLock sync.RWMutex
	locks      map[string]sync.Mutex
}

func (d *InMemoryLocker) Lock(ledger string) (Unlock, error) {
	d.globalLock.RLock()
	lock, ok := d.locks[ledger]
	d.globalLock.RUnlock()
	if ok {
		goto ret
	}

	d.globalLock.Lock()
	lock, ok = d.locks[ledger] // Double check, the lock can have been acquired by another go routing between RUnlock and Lock
	if !ok {
		d.locks[ledger] = lock
	}
	d.globalLock.Unlock()
ret:
	lock.Lock()
	return lock.Unlock, nil
}

func NewInMemoryLocker() *InMemoryLocker {
	return &InMemoryLocker{
		locks: map[string]sync.Mutex{},
	}
}
