package coldstorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ColdReader provides read access to archived period data by downloading SST files
// from cold storage, ingesting them into temporary Pebble databases, and caching
// the opened databases for repeated access.
//
// Entries are evicted when the LRU capacity is exceeded or when they have not been
// accessed for longer than the configured TTL. A background goroutine periodically
// sweeps expired entries; set ttl to 0 to disable time-based eviction.
type ColdReader struct {
	mu          sync.Mutex
	coldStorage ColdStorage
	bucketID    string
	cacheDir    string
	maxCached   int
	ttl         time.Duration
	cache       map[uint64]*cachedPeriod
	lru         []uint64 // eviction order, oldest first
	logger      logging.Logger
	stopSweep   chan struct{}
}

type cachedPeriod struct {
	db         *pebble.DB
	lastAccess time.Time
}

// NewColdReader creates a ColdReader that caches up to maxCached opened Pebble DBs.
// Entries unused for longer than ttl are evicted in the background.
// Set ttl to 0 to disable time-based eviction.
func NewColdReader(
	coldStorage ColdStorage,
	bucketID string,
	cacheDir string,
	maxCached int,
	ttl time.Duration,
	logger logging.Logger,
) *ColdReader {
	r := &ColdReader{
		coldStorage: coldStorage,
		bucketID:    bucketID,
		cacheDir:    cacheDir,
		maxCached:   maxCached,
		ttl:         ttl,
		cache:       make(map[uint64]*cachedPeriod),
		logger:      logger.WithFields(map[string]any{"cmp": "cold-reader"}),
		stopSweep:   make(chan struct{}),
	}

	if ttl > 0 {
		go r.sweepLoop()
	}

	return r
}

// GetReader returns a PebbleReader for the given archived period.
// It downloads and caches the SST file if not already cached.
func (r *ColdReader) GetReader(ctx context.Context, periodID uint64) (dal.PebbleReader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Cache hit
	if cached, ok := r.cache[periodID]; ok {
		cached.lastAccess = time.Now()
		r.touchLRU(periodID)

		return cached.db, nil
	}

	// Cache miss: fetch, ingest, cache
	r.logger.WithFields(map[string]any{"periodId": periodID}).Infof("Fetching archived period from cold storage")

	periodDir := filepath.Join(r.cacheDir, "period-"+strconv.FormatUint(periodID, 10))
	sstPath := filepath.Join(periodDir, "archive.sst")
	dbDir := filepath.Join(periodDir, "db")

	// Download SST
	if err := r.downloadSST(ctx, periodID, sstPath); err != nil {
		return nil, fmt.Errorf("downloading SST for period %d: %w", periodID, err)
	}

	// Open Pebble DB and ingest the SST
	db, err := r.openAndIngest(dbDir, sstPath)
	if err != nil {
		_ = os.RemoveAll(periodDir)

		return nil, fmt.Errorf("ingesting SST for period %d: %w", periodID, err)
	}

	// Evict oldest if at capacity
	if len(r.cache) >= r.maxCached {
		r.evictOldest()
	}

	r.cache[periodID] = &cachedPeriod{db: db, lastAccess: time.Now()}
	r.lru = append(r.lru, periodID)

	return db, nil
}

func (r *ColdReader) downloadSST(ctx context.Context, periodID uint64, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	rc, err := r.coldStorage.Fetch(ctx, r.bucketID, periodID)
	if err != nil {
		return err
	}

	defer func() { _ = rc.Close() }()

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating SST cache file: %w", err)
	}

	defer func() { _ = f.Close() }()

	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("writing SST cache file: %w", err)
	}

	return f.Close()
}

func (r *ColdReader) openAndIngest(dbDir, sstPath string) (*pebble.DB, error) {
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating DB directory: %w", err)
	}

	db, err := pebble.Open(dbDir, &pebble.Options{
		FS:                          vfs.Default,
		DisableWAL:                  true,
		L0CompactionThreshold:       1000, // effectively disable auto-compaction
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               1 << 60,
		MaxOpenFiles:                100,
		MemTableSize:                1 << 20, // 1MB — minimal since we only ingest
		MemTableStopWritesThreshold: 4,
	})
	if err != nil {
		return nil, fmt.Errorf("opening Pebble DB: %w", err)
	}

	if err := db.Ingest([]string{sstPath}); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("ingesting SST: %w", err)
	}

	return db, nil
}

func (r *ColdReader) touchLRU(periodID uint64) {
	for i, id := range r.lru {
		if id == periodID {
			r.lru = append(r.lru[:i], r.lru[i+1:]...)
			r.lru = append(r.lru, periodID)

			return
		}
	}
}

func (r *ColdReader) evictOldest() {
	if len(r.lru) == 0 {
		return
	}

	oldest := r.lru[0]
	r.lru = r.lru[1:]

	if cached, ok := r.cache[oldest]; ok {
		r.logger.WithFields(map[string]any{"periodId": oldest}).Infof("Evicting cached period")

		_ = cached.db.Close()
		delete(r.cache, oldest)

		periodDir := filepath.Join(r.cacheDir, "period-"+strconv.FormatUint(oldest, 10))
		_ = os.RemoveAll(periodDir)
	}
}

// sweepLoop periodically evicts cache entries that have not been accessed within the TTL.
func (r *ColdReader) sweepLoop() {
	ticker := time.NewTicker(r.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopSweep:
			return
		case <-ticker.C:
			r.sweepExpired()
		}
	}
}

func (r *ColdReader) sweepExpired() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	expired := make([]uint64, 0)

	for id, cached := range r.cache {
		if now.Sub(cached.lastAccess) > r.ttl {
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		r.evictByID(id)
	}
}

func (r *ColdReader) evictByID(id uint64) {
	cached, ok := r.cache[id]
	if !ok {
		return
	}

	r.logger.WithFields(map[string]any{"periodId": id}).Infof("Evicting expired cached period")

	_ = cached.db.Close()
	delete(r.cache, id)

	// Remove from LRU slice
	for i, lruID := range r.lru {
		if lruID == id {
			r.lru = append(r.lru[:i], r.lru[i+1:]...)

			break
		}
	}

	periodDir := filepath.Join(r.cacheDir, "period-"+strconv.FormatUint(id, 10))
	_ = os.RemoveAll(periodDir)
}

// Close closes all cached Pebble databases and removes the cache directory contents.
func (r *ColdReader) Close() error {
	close(r.stopSweep)

	r.mu.Lock()
	defer r.mu.Unlock()

	for id, cached := range r.cache {
		_ = cached.db.Close()

		periodDir := filepath.Join(r.cacheDir, "period-"+strconv.FormatUint(id, 10))
		_ = os.RemoveAll(periodDir)
	}

	r.cache = make(map[uint64]*cachedPeriod)
	r.lru = nil

	return nil
}
