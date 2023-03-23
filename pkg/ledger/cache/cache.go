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
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"golang.org/x/sync/singleflight"
)

type Store interface {
	GetAccountWithVolumes(ctx context.Context, addr string) (*core.AccountWithVolumes, error)
}
type AccountComputerFn func(ctx context.Context, address string) (*core.AccountWithVolumes, error)

func (fn AccountComputerFn) ComputeAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	return fn(ctx, address)
}

type cacheEntry struct {
	sync.Mutex
	account  *core.AccountWithVolumes
	lastUsed time.Time
	inUse    atomic.Int64
}

type Release func()

// TODO(gfyrag): Add a routine to evict all entries
// and update metrics:
// c.metricsRegistry.CacheNumberEntries.Add(ctx, -1) for all evicted entries
type Cache struct {
	mu              sync.RWMutex
	cache           map[string]*cacheEntry
	store           Store
	sg              singleflight.Group
	metricsRegistry metrics.PerLedgerMetricsRegistry
}

func (c *Cache) getEntry(ctx context.Context, address string) (*cacheEntry, error) {
	item, err, _ := c.sg.Do(address, func() (interface{}, error) {
		c.mu.RLock()
		entry, ok := c.cache[address]
		c.mu.RUnlock()
		if !ok {
			// cache miss
			c.metricsRegistry.CacheMisses().Add(ctx, 1)

			account, err := c.store.GetAccountWithVolumes(ctx, address)
			if err != nil {
				return nil, err
			}

			entry = &cacheEntry{
				account:  account,
				lastUsed: time.Now(),
			}
			c.mu.Lock()
			c.cache[address] = entry
			c.mu.Unlock()

			if c.metricsRegistry != nil {
				c.metricsRegistry.CacheNumberEntries().Add(ctx, +1)
			}

			return entry, nil
		}
		return entry, nil
	})
	return item.(*cacheEntry), err
}

func (c *Cache) LockAccounts(ctx context.Context, address ...string) (Release, error) {
	entries := make([]*cacheEntry, 0)
	for _, address := range address {
		entry, err := c.getEntry(ctx, address)
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

	entry, err := c.getEntry(ctx, address)
	if err != nil {
		return nil, err
	}
	entry.Lock()
	defer entry.Unlock()
	cp := entry.account.Copy()

	return &cp, nil
}

func (c *Cache) withLockOnAccount(address string, callback func(account *core.AccountWithVolumes)) {
	c.mu.Lock()
	entry, ok := c.cache[address]
	c.mu.Unlock()
	if !ok {
		panic("cache empty for address: " + address)
	}
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

func New(store Store, metricsRegistry metrics.PerLedgerMetricsRegistry) *Cache {
	return &Cache{
		store:           store,
		cache:           map[string]*cacheEntry{},
		metricsRegistry: metricsRegistry,
	}
}
