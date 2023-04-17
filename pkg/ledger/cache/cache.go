package cache

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

const (
	DefaultRetainDelay    = time.Minute
	DefaultEvictionPeriod = 5 * time.Minute
)

var (
	ErrStopped = errors.New("cache stopped")
)

type Store interface {
	GetAccountWithVolumes(ctx context.Context, addr string) (*core.AccountWithVolumes, error)
}

type cacheEntry struct {
	sync.Mutex
	account  *core.AccountWithVolumes
	lastUsed time.Time
	inUse    atomic.Int64
	ready    chan struct{}
}

type Release func()

type Cache struct {
	cache           sync.Map
	store           Store
	metricsRegistry metrics.PerLedgerMetricsRegistry
	mu              sync.RWMutex
	stop            chan chan struct{}
	stopped         chan struct{}
	evictionPeriod  time.Duration
	retainDelay     time.Duration
}

func (c *Cache) loadEntry(ctx context.Context, address string, inUse bool) (*cacheEntry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ce := &cacheEntry{
		ready: make(chan struct{}),
	}
	entry, loaded := c.cache.LoadOrStore(address, ce)
	if !loaded {
		account, err := c.store.GetAccountWithVolumes(ctx, address)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			panic(err)
		}
		if errors.Is(err, storage.ErrNotFound) {
			account = core.NewAccountWithVolumes(address)
		}
		ce.account = account

		close(ce.ready)
		// cache miss
		c.metricsRegistry.CacheMisses().Add(ctx, 1)
		c.metricsRegistry.CacheNumberEntries().Add(ctx, 1)
	}

	ce = entry.(*cacheEntry)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stopped:
		return nil, ErrStopped
	case <-ce.ready:
	}

	ce.lastUsed = time.Now()
	if inUse {
		ce.inUse.Add(1)
	}

	return ce, nil
}

func (c *Cache) Count() int {
	counter := 0
	c.cache.Range(func(key, value any) bool {
		counter++
		return true
	})
	return counter
}

func (c *Cache) LockAccounts(ctx context.Context, address ...string) (Release, error) {
	entries := make([]*cacheEntry, 0)
	for _, address := range address {
		entry, err := c.loadEntry(ctx, address, true)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	released := false
	return func() {
		if released {
			return
		}
		released = true
		for _, entry := range entries {
			entry.inUse.Add(-1)
		}
	}, nil
}

func (c *Cache) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	address = strings.TrimPrefix(address, "@")

	entry, err := c.loadEntry(ctx, address, false)
	if err != nil {
		return nil, err
	}
	entry.Lock()
	defer entry.Unlock()
	cp := entry.account.Copy()

	return &cp, nil
}

func (c *Cache) withLockOnAccount(address string, callback func(account *core.AccountWithVolumes)) {
	e, ok := c.cache.Load(address)
	if !ok {
		panic("cache empty for address: " + address)
	}
	entry := e.(*cacheEntry)
	entry.Lock()
	defer entry.Unlock()

	callback(entry.account)
}

func (c *Cache) addOutput(address, asset string, amount *big.Int) {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		volumes := account.Volumes[asset].CopyWithZerosIfNeeded()
		volumes.Output.Add(volumes.Output, amount)
		account.Volumes[asset] = volumes
	})
}

func (c *Cache) addInput(address, asset string, amount *big.Int) {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		if account == nil {
			panic("account " + address + " is nil")
		}
		volumes := account.Volumes[asset].CopyWithZerosIfNeeded()
		volumes.Input.Add(volumes.Input, amount)
		account.Volumes[asset] = volumes
	})
}

func (c *Cache) UpdateVolumeWithTX(tx *core.Transaction) {
	for _, posting := range tx.Postings {
		c.addOutput(posting.Source, posting.Asset, posting.Amount)
		c.addInput(posting.Destination, posting.Asset, posting.Amount)
	}
}

func (c *Cache) UpdateAccountMetadata(address string, m metadata.Metadata) error {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		account.Metadata = account.Metadata.Merge(m)
	})

	return nil
}

func (c *Cache) Stop(ctx context.Context) error {
	ch := make(chan struct{})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.stop <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return nil
		}
	}
}

func (c *Cache) Run(ctx context.Context) error {
	for {
		dirty := make(map[string]*cacheEntry)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case stop := <-c.stop:
			close(c.stopped)
			close(stop)
			return nil
		case <-time.After(c.evictionPeriod):
			c.cache.Range(func(key, value any) bool {
				entry := value.(*cacheEntry)
				if entry.inUse.Load() == 0 && time.Since(entry.lastUsed) > c.retainDelay {
					dirty[key.(string)] = entry
				}
				return true
			})
			if len(dirty) > 0 {
				func() {
					c.mu.Lock()
					defer c.mu.Unlock()

					for addr, entry := range dirty {
						if entry.inUse.Load() > 0 {
							continue
						}
						c.cache.Delete(addr)
						c.metricsRegistry.CacheNumberEntries().Add(ctx, -1)
					}
				}()
			}
		}
	}
}

type Option func(c *Cache)

func WithRetainDelay(t time.Duration) Option {
	return func(c *Cache) {
		c.retainDelay = t
	}
}

func WithEvictionPeriod(t time.Duration) Option {
	return func(c *Cache) {
		c.evictionPeriod = t
	}
}

func WithMetricsRegistry(registry metrics.PerLedgerMetricsRegistry) Option {
	return func(c *Cache) {
		c.metricsRegistry = registry
	}
}

var defaultOptions = []Option{
	WithRetainDelay(DefaultRetainDelay),
	WithEvictionPeriod(DefaultEvictionPeriod),
	WithMetricsRegistry(metrics.NewNoOpMetricsRegistry()),
}

func New(store Store, options ...Option) *Cache {
	c := &Cache{
		store:   store,
		stop:    make(chan chan struct{}),
		stopped: make(chan struct{}),
	}
	for _, opt := range append(defaultOptions, options...) {
		opt(c)
	}

	return c
}
