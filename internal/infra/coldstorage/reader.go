package coldstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ColdReader provides read access to archived chapter data by downloading SST files
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
	cache       map[uint64]*cachedChapter
	lru         []uint64 // eviction order, oldest first
	logger      logging.Logger
	stopSweep   chan struct{}
	closed      bool
}

type cachedChapter struct {
	db              *pebble.DB
	lastAccess      time.Time
	leases          int
	evictionPending bool
}

// ErrReaderClosed is returned when a read is requested after ColdReader.Close.
var ErrReaderClosed = errors.New("cold reader is closed")

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
		cache:       make(map[uint64]*cachedChapter),
		logger:      logger.WithFields(map[string]any{"cmp": "cold-reader"}),
		stopSweep:   make(chan struct{}),
	}

	if ttl > 0 {
		go r.sweepLoop()
	}

	return r
}

// AcquireReader returns a cached chapter reader protected from LRU, TTL, and
// shutdown eviction until release is called. release is idempotent, reports
// impossible lease-state violations, and must be called as soon as the caller
// finishes every point lookup and iterator that uses the returned reader.
func (r *ColdReader) AcquireReader(
	ctx context.Context,
	chapterID uint64,
) (dal.PebbleReader, func() error, error) {
	r.mu.Lock()
	cached, err := r.getOrLoadLocked(ctx, chapterID)
	if err != nil {
		r.mu.Unlock()

		return nil, nil, err
	}
	cached.leases++
	r.mu.Unlock()

	var (
		once       sync.Once
		releaseErr error
	)
	release := func() error {
		once.Do(func() {
			releaseErr = r.releaseChapter(chapterID, cached)
		})

		return releaseErr
	}

	return cached.db, release, nil
}

// getOrLoadLocked returns the cached chapter, downloading and ingesting it on
// a miss. The caller must hold r.mu.
func (r *ColdReader) getOrLoadLocked(ctx context.Context, chapterID uint64) (*cachedChapter, error) {
	if r.closed {
		return nil, ErrReaderClosed
	}

	// Cache hit
	if cached, ok := r.cache[chapterID]; ok {
		cached.lastAccess = time.Now()
		r.touchLRU(chapterID)

		return cached, nil
	}

	// Cache miss: fetch, ingest, cache
	r.logger.WithFields(map[string]any{"chapterId": chapterID}).Infof("Fetching archived chapter from cold storage")

	chapterDir := filepath.Join(r.cacheDir, "chapter-"+strconv.FormatUint(chapterID, 10))
	sstPath := filepath.Join(chapterDir, "archive.sst")
	dbDir := filepath.Join(chapterDir, "db")

	// Download SST
	if err := r.downloadSST(ctx, chapterID, sstPath); err != nil {
		return nil, fmt.Errorf("downloading SST for chapter %d: %w", chapterID, err)
	}

	// Open Pebble DB and ingest the SST
	db, err := r.openAndIngest(dbDir, sstPath)
	if err != nil {
		_ = os.RemoveAll(chapterDir)

		return nil, fmt.Errorf("ingesting SST for chapter %d: %w", chapterID, err)
	}

	// Evict unleased entries before inserting. If every candidate is leased,
	// temporarily exceed the capacity; releaseChapter will converge the cache
	// once a lease ends.
	for len(r.cache) >= r.capacity() {
		before := len(r.cache)
		r.evictOldest()
		if len(r.cache) == before {
			break
		}
	}

	cached := &cachedChapter{db: db, lastAccess: time.Now()}
	r.cache[chapterID] = cached
	r.lru = append(r.lru, chapterID)

	return cached, nil
}

func (r *ColdReader) capacity() int {
	if r.maxCached <= 0 {
		return 1
	}

	return r.maxCached
}

func (r *ColdReader) releaseChapter(chapterID uint64, leased *cachedChapter) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cached, ok := r.cache[chapterID]
	if !ok {
		return fmt.Errorf("invariant: releasing cold reader lease for chapter %d but cache entry is absent", chapterID)
	}
	if cached != leased {
		return fmt.Errorf("invariant: releasing stale cold reader lease for chapter %d", chapterID)
	}
	if cached.leases <= 0 {
		return fmt.Errorf("invariant: releasing cold reader lease for chapter %d with zero refcount", chapterID)
	}
	cached.leases--
	cached.lastAccess = time.Now()
	if cached.leases > 0 {
		return nil
	}
	if r.closed || cached.evictionPending {
		r.evictByID(chapterID)

		return nil
	}

	r.evictToCapacity()

	return nil
}

// evictToCapacity removes the oldest unleased entries until the configured
// bound is restored. The caller must hold r.mu.
func (r *ColdReader) evictToCapacity() {
	for len(r.cache) > r.capacity() {
		before := len(r.cache)
		r.evictOldest()
		if len(r.cache) == before {
			return
		}
	}
}

func (r *ColdReader) downloadSST(ctx context.Context, chapterID uint64, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	rc, err := r.coldStorage.Fetch(ctx, r.bucketID, chapterID)
	if err != nil {
		return err
	}

	defer func() { _ = rc.Close() }()

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating SST cache file: %w", err)
	}

	if _, err := io.Copy(f, rc); err != nil {
		_ = f.Close()

		return fmt.Errorf("writing SST cache file: %w", err)
	}

	// Close explicitly (not deferred) so the flush error is propagated.
	return f.Close()
}

func (r *ColdReader) openAndIngest(dbDir, sstPath string) (*pebble.DB, error) {
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating DB directory: %w", err)
	}

	db, err := pebble.Open(dbDir, &pebble.Options{
		Logger:                      dal.NewPebbleLogger(r.logger),
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

	if err := db.Ingest(context.Background(), []string{sstPath}); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("ingesting SST: %w", err)
	}

	return db, nil
}

func (r *ColdReader) touchLRU(chapterID uint64) {
	for i, id := range r.lru {
		if id == chapterID {
			r.lru = append(r.lru[:i], r.lru[i+1:]...)
			r.lru = append(r.lru, chapterID)

			return
		}
	}
}

func (r *ColdReader) evictOldest() {
	for _, id := range r.lru {
		cached, ok := r.cache[id]
		if !ok {
			continue
		}
		if cached.leases > 0 {
			cached.evictionPending = true

			continue
		}

		r.evictByID(id)

		return
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
			if cached.leases > 0 {
				cached.evictionPending = true

				continue
			}
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
	if cached.leases > 0 {
		cached.evictionPending = true

		return
	}

	r.logger.WithFields(map[string]any{"chapterId": id}).Infof("Evicting cached chapter")

	_ = cached.db.Close()
	delete(r.cache, id)

	// Remove from LRU slice
	for i, lruID := range r.lru {
		if lruID == id {
			r.lru = append(r.lru[:i], r.lru[i+1:]...)

			break
		}
	}

	chapterDir := filepath.Join(r.cacheDir, "chapter-"+strconv.FormatUint(id, 10))
	_ = os.RemoveAll(chapterDir)
}

// Close prevents new reads, closes every unleased Pebble database, and marks
// leased chapters for cleanup by their final release callback. It is idempotent.
func (r *ColdReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.stopSweep)

	ids := make([]uint64, 0, len(r.cache))
	for id := range r.cache {
		ids = append(ids, id)
	}
	for _, id := range ids {
		r.evictByID(id)
	}

	return nil
}
