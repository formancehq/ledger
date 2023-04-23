package command

import (
	"context"
	"sync"
	"sync/atomic"
)

type Unlock func(ctx context.Context)

type Locker interface {
	Lock(ctx context.Context, accounts Accounts) (Unlock, error)
}
type LockerFn func(ctx context.Context, accounts Accounts) (Unlock, error)

func (fn LockerFn) Lock(ctx context.Context, accounts Accounts) (Unlock, error) {
	return fn(ctx, accounts)
}

var NoOpLocker = LockerFn(func(ctx context.Context, accounts Accounts) (Unlock, error) {
	return func(ctx context.Context) {}, nil
})

type Accounts struct {
	Read  []string
	Write []string
}

type linkedListItem[T any] struct {
	next     *linkedListItem[T]
	previous *linkedListItem[T]
	value    T
	list     *linkedList[T]
}

func (i *linkedListItem[T]) Remove() {
	if i.previous == nil {
		i.list.first = i.next
		return
	}
	if i.next == nil {
		i.list.last = i.previous
		return
	}
	i.previous.next, i.next.previous = i.next, i.previous
}

type linkedList[T any] struct {
	first *linkedListItem[T]
	last  *linkedListItem[T]
}

func (l *linkedList[T]) PutTail(v T) *linkedListItem[T] {
	item := &linkedListItem[T]{
		previous: l.last,
		value:    v,
		list:     l,
	}
	l.last = item

	return item
}

type lockIntent struct {
	accounts Accounts
	acquired chan struct{}
}

func (intent *lockIntent) tryLock(chain *DefaultLocker) bool {

	for _, account := range intent.accounts.Read {
		_, ok := chain.writeLocks[account]
		if ok {
			return false
		}
	}

	for _, account := range intent.accounts.Write {
		_, ok := chain.readLocks[account]
		if ok {
			return false
		}
		_, ok = chain.writeLocks[account]
		if ok {
			return false
		}
	}

	for _, account := range intent.accounts.Read {
		atomicValue, ok := chain.readLocks[account]
		if !ok {
			atomicValue = &atomic.Int64{}
			chain.readLocks[account] = atomicValue
		}
		atomicValue.Add(1)
	}
	for _, account := range intent.accounts.Write {
		chain.writeLocks[account] = struct{}{}
	}

	return true
}

func (intent *lockIntent) unlock(chain *DefaultLocker) {
	for _, account := range intent.accounts.Read {
		atomicValue := chain.readLocks[account]
		if atomicValue.Add(-1) == 0 {
			delete(chain.readLocks, account)
		}
	}
	for _, account := range intent.accounts.Write {
		delete(chain.writeLocks, account)
	}
}

type DefaultLocker struct {
	intents    *linkedList[*lockIntent]
	mu         sync.Mutex
	readLocks  map[string]*atomic.Int64
	writeLocks map[string]struct{}
}

func (defaultLocker *DefaultLocker) Lock(ctx context.Context, accounts Accounts) (Unlock, error) {
	defaultLocker.mu.Lock()
	intent := &lockIntent{
		accounts: accounts,
		acquired: make(chan struct{}),
	}
	item := defaultLocker.intents.PutTail(intent)
	acquired := intent.tryLock(defaultLocker)
	defaultLocker.mu.Unlock()

	if !acquired {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-intent.acquired:
		}
	}

	return func(ctx context.Context) {
		defaultLocker.mu.Lock()
		defer defaultLocker.mu.Unlock()

		intent.unlock(defaultLocker)
		item.Remove()

		for next := item.next; next != nil; {
			if next.value.tryLock(defaultLocker) {
				close(next.value.acquired)
			}
		}
	}, nil
}

func NewDefaultLocker() *DefaultLocker {
	return &DefaultLocker{
		intents:    &linkedList[*lockIntent]{},
		readLocks:  map[string]*atomic.Int64{},
		writeLocks: map[string]struct{}{},
	}
}
