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

type Store interface {
	GetAccountWithVolumes(ctx context.Context, addr string) (*core.AccountWithVolumes, error)
}

type cacheEntry struct {
	sync.Mutex
	account  *core.AccountWithVolumes
	lastUsed time.Time
	inUse    atomic.Int64
	ready    chan struct{}
	evicted chan struct{}
}

type Release func()

// TODO(gfyrag): Add a routine to evict all entries
// and update metrics:
// c.metricsRegistry.CacheNumberEntries.Add(ctx, -1) for all evicted entries
type Cache struct {
	cache           sync.Map
	store           Store
	metricsRegistry metrics.PerLedgerMetricsRegistry
	counter         atomic.Int64
}

func (c *Cache) loadEntry(ctx context.Context, address string, inUse bool) (*cacheEntry, error) {

	ce := &cacheEntry{
		ready: make(chan struct{}),
		evicted: make(chan struct{}),
	}
	entry, loaded := c.cache.LoadOrStore(address, ce)
	if !loaded {
		// cache miss
		c.metricsRegistry.CacheMisses().Add(ctx, 1)

		account, err := c.store.GetAccountWithVolumes(ctx, address)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			panic(err)
		}
		if errors.Is(err, storage.ErrNotFound) {
			account = core.NewAccountWithVolumes(address)
		}
		ce.account = account

		close(ce.ready)
		c.metricsRegistry.CacheNumberEntries().Add(ctx, 1)
	}

	ce = entry.(*cacheEntry)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ce.ready:
	case <-ce.evicted:
		return c.loadEntry(ctx, address, inUse)
	}

	ce.lastUsed = time.Now()
	if inUse {
		ce.inUse.Add(1)
	}

	return ce, nil
}

func (c *Cache) LockAccounts(ctx context.Context, address ...string) (Release, error) {
	entries := make([]*cacheEntry, 0)
	for _, address := range address {
		entry, err := c.loadEntry(ctx, address, true)
		if err != nil {
			return nil, err
		}
		entry.inUse.Add(1)
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
		volumes := account.Volumes[asset].CopyWithZerosIfNeeded()
		volumes.Input.Add(volumes.Input, amount)
		account.Volumes[asset] = volumes
	})
}

func (c *Cache) UpdateVolumeWithTX(tx core.Transaction) {
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

//func (c *Cache) runEviction() {
//	for {
//		select {
//		case <-time.After(time.Minute):
//			c.cache.Range(func(key, value any) bool {
//				cacheEntry := value.(*cacheEntry)
//				if cacheEntry.inUse.Load() == 0 {
//
//				}
//				return true
//			})
//		}
//	}
//}

func New(store Store, metricsRegistry metrics.PerLedgerMetricsRegistry) *Cache {
	if metricsRegistry == nil {
		metricsRegistry = metrics.NewNoOpMetricsRegistry()
	}
	c := &Cache{
		store:           store,
		metricsRegistry: metricsRegistry,
	}
	go func() {
		for {
			c.cache.Range(func(key, value any) bool {
				entry := value.(*cacheEntry)
				if entry.inUse.Load() == 0 && entry.lastUsed.Before(time.Now().Add(-time.Minute)) {
					entry.
				}
			})
		}
	}()

	return c
}
