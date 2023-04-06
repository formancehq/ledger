package lock

import (
	"context"
)

type Unlock func(ctx context.Context)

type Accounts struct {
	Read  []string
	Write []string
}

type lockQuery struct {
	accounts Accounts
	ready    chan Unlock
}

type unlockQuery struct {
	accounts Accounts
	done     chan struct{}
}

type Locker struct {
	readLocks     map[string]struct{}
	writeLocks    map[string]struct{}
	ledger        string
	lockQueries   chan lockQuery
	unlockQueries chan unlockQuery
	pending       []*lockQuery
	stopChan      chan chan struct{}
}

func (d *Locker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case query := <-d.unlockQueries:
			d.unlock(ctx, query.accounts)
			close(query.done)
			d.tryNext(ctx)
		case query := <-d.lockQueries:
			if d.process(ctx, query) {
				continue
			}
			d.pending = append(d.pending, &query)
		case ch := <-d.stopChan:
			close(ch)
			return nil
		}
	}
}

func (d *Locker) process(ctx context.Context, query lockQuery) bool {
	unlock, acquired := d.tryLock(ctx, query.accounts)
	if acquired {
		query.ready <- unlock
		return true
	}
	return false
}

func (d *Locker) tryNext(ctx context.Context) {
	for _, query := range d.pending {
		if d.process(ctx, *query) {
			return
		}
	}
}

func (d *Locker) tryLock(ctx context.Context, accounts Accounts) (Unlock, bool) {

	for _, account := range accounts.Read {
		_, ok := d.writeLocks[account]
		if ok {
			return nil, false
		}
	}

	for _, account := range accounts.Write {
		_, ok := d.readLocks[account]
		if ok {
			return nil, false
		}
		_, ok = d.writeLocks[account]
		if ok {
			return nil, false
		}
	}

	for _, account := range accounts.Read {
		d.readLocks[account] = struct{}{}
	}
	for _, account := range accounts.Write {
		d.writeLocks[account] = struct{}{}
	}

	return func(ctx context.Context) {
		q := unlockQuery{
			accounts: accounts,
			done:     make(chan struct{}),
		}
		d.unlockQueries <- q
		select {
		case <-ctx.Done():
		case <-q.done:
		}
	}, true
}

func (d *Locker) unlock(ctx context.Context, accounts Accounts) {
	for _, account := range accounts.Read {
		delete(d.readLocks, account)
	}
	for _, account := range accounts.Write {
		delete(d.writeLocks, account)
	}
}

func (d *Locker) Lock(ctx context.Context, accounts Accounts) (Unlock, error) {
	q := lockQuery{
		accounts: accounts,
		ready:    make(chan Unlock, 1),
	}
	d.lockQueries <- q
	return <-q.ready, nil
}

func (d *Locker) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case d.stopChan <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return nil
		}
	}
}

func New(ledger string) *Locker {
	return &Locker{
		readLocks:     map[string]struct{}{},
		writeLocks:    map[string]struct{}{},
		ledger:        ledger,
		lockQueries:   make(chan lockQuery),
		unlockQueries: make(chan unlockQuery),
		stopChan:      make(chan chan struct{}),
	}
}
