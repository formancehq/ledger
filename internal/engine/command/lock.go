package command

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
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

type lockIntent struct {
	accounts Accounts
	acquired chan struct{}
	at       time.Time
}

func (intent *lockIntent) tryLock(ctx context.Context, chain *DefaultLocker) bool {

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

	logging.FromContext(ctx).Debugf("Lock acquired")

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

func (intent *lockIntent) unlock(ctx context.Context, chain *DefaultLocker) {
	logging.FromContext(ctx).Debugf("Unlock accounts")
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
	intents    *collectionutils.LinkedList[*lockIntent]
	mu         sync.Mutex
	readLocks  map[string]*atomic.Int64
	writeLocks map[string]struct{}
}

func (defaultLocker *DefaultLocker) Lock(ctx context.Context, accounts Accounts) (Unlock, error) {
	defaultLocker.mu.Lock()

	logger := logging.FromContext(ctx).WithFields(map[string]any{
		"read":  accounts.Read,
		"write": accounts.Write,
	})
	ctx = logging.ContextWithLogger(ctx, logger)

	logger.Debugf("Intent lock")
	intent := &lockIntent{
		accounts: accounts,
		acquired: make(chan struct{}),
		at:       time.Now(),
	}

	recheck := func() {
		node := defaultLocker.intents.FirstNode()
		for {
			if node == nil {
				return
			}
			if node.Value().tryLock(ctx, defaultLocker) {
				node.Remove()
				close(node.Value().acquired)
				return
			}
			node = node.Next()
		}
	}

	releaseIntent := func(ctx context.Context) {
		defaultLocker.mu.Lock()
		defer defaultLocker.mu.Unlock()

		intent.unlock(logging.ContextWithLogger(ctx, logger), defaultLocker)

		recheck()
	}

	acquired := intent.tryLock(ctx, defaultLocker)
	if acquired {
		logger.Debugf("Lock directly acquired")
		defaultLocker.mu.Unlock()

		return releaseIntent, nil
	}

	logger.Debugf("Lock not acquired, some accounts are already used, putting in queue")
	defaultLocker.intents.Append(intent)
	defaultLocker.mu.Unlock()

	select {
	case <-ctx.Done():
		defaultLocker.intents.RemoveValue(intent)
		return nil, errors.Wrapf(ctx.Err(), "locking accounts: %s as read, and %s as write", accounts.Read, accounts.Write)
	case <-intent.acquired:
		return releaseIntent, nil
	}
}

func NewDefaultLocker() *DefaultLocker {
	return &DefaultLocker{
		intents:    collectionutils.NewLinkedList[*lockIntent](),
		readLocks:  map[string]*atomic.Int64{},
		writeLocks: map[string]struct{}{},
	}
}
