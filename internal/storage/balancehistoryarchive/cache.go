package balancehistoryarchive

import (
	"container/list"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const cacheFileSuffix = ".run"

type cache struct {
	mu       sync.Mutex
	dir      string
	maxBytes int64
	bytes    int64
	entries  map[[32]byte]*cacheEntry
	lru      *list.List
	inflight map[[32]byte]*cacheCall
}

type cacheEntry struct {
	ref        Ref
	path       string
	size       int64
	indexBytes int64
	leases     int
	invalid    bool
	element    *list.Element
	indexMu    sync.Mutex
	index      *recordIndex
}

type cacheCall struct {
	done chan struct{}
	err  error
}

type preparedFile struct {
	path string
	size int64
}

type scannedFile struct {
	ref     Ref
	path    string
	size    int64
	modTime int64
}

// CacheStats is a bounded-cardinality snapshot useful for health reporting
// and deterministic cache tests.
type CacheStats struct {
	Bytes   int64 `json:"bytes"`
	Entries int   `json:"entries"`
	Leases  int   `json:"leases"`
}

func newCache(dir string, maxBytes int64) (*cache, error) {
	if dir == "" {
		return nil, errors.New("balance history archive cache directory is required")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("balance history archive cache max bytes must be positive, got %d", maxBytes)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating balance history archive cache: %w", err)
	}

	c := &cache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  make(map[[32]byte]*cacheEntry),
		lru:      list.New(),
		inflight: make(map[[32]byte]*cacheCall),
	}
	if err := c.scan(); err != nil {
		return nil, err
	}
	if err := c.evictLocked(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *cache) scan() error {
	directoryEntries, err := os.ReadDir(c.dir)
	if err != nil {
		return fmt.Errorf("reading balance history archive cache: %w", err)
	}

	files := make([]scannedFile, 0, len(directoryEntries))
	for _, directoryEntry := range directoryEntries {
		if directoryEntry.IsDir() || !strings.HasSuffix(directoryEntry.Name(), cacheFileSuffix) {
			continue
		}

		hexed := strings.TrimSuffix(directoryEntry.Name(), cacheFileSuffix)
		if len(hexed) != hex.EncodedLen(sha256Length) {
			continue
		}
		digest, err := hex.DecodeString(hexed)
		if err != nil || len(digest) != sha256Length {
			continue
		}

		info, err := directoryEntry.Info()
		if err != nil {
			return fmt.Errorf("stating cached run %s: %w", directoryEntry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			continue
		}

		var sha [sha256Length]byte
		copy(sha[:], digest)
		files = append(files, scannedFile{
			ref:     Ref{SHA256: sha, Size: uint64(info.Size())},
			path:    filepath.Join(c.dir, directoryEntry.Name()),
			size:    info.Size(),
			modTime: info.ModTime().UnixNano(),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime == files[j].modTime {
			return files[i].path < files[j].path
		}

		return files[i].modTime < files[j].modTime
	})
	for _, file := range files {
		entry := &cacheEntry{ref: file.ref, path: file.path, size: file.size}
		entry.element = c.lru.PushBack(entry)
		c.entries[file.ref.SHA256] = entry
		c.bytes += file.size
	}

	return nil
}

func (c *cache) acquire(
	ctx context.Context,
	ref Ref,
	load func() (preparedFile, error),
) (*Lease, bool, error) {
	for {
		c.mu.Lock()
		if entry, ok := c.entries[ref.SHA256]; ok && !entry.invalid {
			// The digest names the file, while the complete reference binds its
			// version, size, and record count for verification by the caller.
			entry.ref = ref
			entry.leases++
			c.touchLocked(entry)
			lease := c.newLeaseLocked(entry)
			c.mu.Unlock()

			return lease, true, nil
		}

		if call, ok := c.inflight[ref.SHA256]; ok {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-call.done:
				if call.err != nil {
					return nil, false, call.err
				}

				continue
			}
		}

		call := &cacheCall{done: make(chan struct{})}
		c.inflight[ref.SHA256] = call
		c.mu.Unlock()

		prepared, loadErr := load()

		c.mu.Lock()
		if loadErr == nil {
			var entry *cacheEntry
			entry, loadErr = c.publishLocked(prepared, ref, 1)
			if loadErr == nil {
				call.err = nil
				delete(c.inflight, ref.SHA256)
				close(call.done)
				lease := c.newLeaseLocked(entry)
				c.mu.Unlock()

				return lease, false, nil
			}
		}

		call.err = loadErr
		delete(c.inflight, ref.SHA256)
		close(call.done)
		c.mu.Unlock()
		if prepared.path != "" {
			// The file was never published, so it is safe to discard it.
			_ = os.Remove(prepared.path)
		}

		return nil, false, loadErr
	}
}

func (c *cache) admit(prepared preparedFile, ref Ref) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[ref.SHA256]; ok && !entry.invalid {
		// The same content-addressed object is already admitted.
		_ = os.Remove(prepared.path)
		c.touchLocked(entry)

		return nil
	}

	_, err := c.publishLocked(prepared, ref, 0)

	return err
}

func (c *cache) publishLocked(prepared preparedFile, ref Ref, leases int) (*cacheEntry, error) {
	if prepared.path == "" {
		return nil, errors.New("prepared cache file path is required")
	}
	if prepared.size < 0 || uint64(prepared.size) != ref.Size {
		return nil, fmt.Errorf("prepared cache file size mismatch: expected %d, got %d", ref.Size, prepared.size)
	}

	finalPath := c.path(ref.SHA256)
	if _, err := os.Stat(finalPath); err == nil {
		if verifyErr := verifyFile(finalPath, ref); verifyErr != nil {
			return nil, verifyErr
		}
		// A previous process already published exactly this immutable blob.
		_ = os.Remove(prepared.path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stating final balance history cache file: %w", err)
	} else {
		if err := os.Chmod(prepared.path, 0o444); err != nil {
			return nil, fmt.Errorf("making prepared balance history cache file immutable: %w", err)
		}
		if err := os.Rename(prepared.path, finalPath); err != nil {
			return nil, fmt.Errorf("publishing balance history cache file: %w", err)
		}
		if err := fsyncDirectory(c.dir); err != nil {
			return nil, fmt.Errorf("syncing balance history cache directory: %w", err)
		}
	}

	entry := &cacheEntry{
		ref:    ref,
		path:   finalPath,
		size:   prepared.size,
		leases: leases,
	}
	entry.element = c.lru.PushBack(entry)
	c.entries[ref.SHA256] = entry
	c.bytes += prepared.size
	if err := c.evictLocked(); err != nil {
		return nil, err
	}

	return entry, nil
}

func (c *cache) path(digest [sha256Length]byte) string {
	return filepath.Join(c.dir, hex.EncodeToString(digest[:])+cacheFileSuffix)
}

func (c *cache) touchLocked(entry *cacheEntry) {
	c.lru.MoveToBack(entry.element)
}

func (c *cache) evictLocked() error {
	for c.bytes > c.maxBytes {
		var candidate *cacheEntry
		for element := c.lru.Front(); element != nil; element = element.Next() {
			entry := element.Value.(*cacheEntry)
			if entry.leases == 0 {
				candidate = entry

				break
			}
		}
		if candidate == nil {
			// Active leases are allowed to put the cache temporarily over its
			// byte budget. Release immediately retries eviction.
			return nil
		}
		if err := c.removeLocked(candidate); err != nil {
			return err
		}
	}

	return nil
}

func (c *cache) removeLocked(entry *cacheEntry) error {
	if entry.leases != 0 {
		return errors.New("invariant: cannot evict a leased balance history archive")
	}
	if err := os.Remove(entry.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("evicting balance history cache file: %w", err)
	}
	c.lru.Remove(entry.element)
	delete(c.entries, entry.ref.SHA256)
	c.bytes -= entry.size + entry.indexBytes
	if c.bytes < 0 {
		return errors.New("invariant: negative balance history archive cache bytes")
	}

	return fsyncDirectory(c.dir)
}

func (c *cache) installIndex(entry *cacheEntry, index *recordIndex) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	current, ok := c.entries[entry.ref.SHA256]
	if !ok || current != entry || entry.invalid {
		return errors.New("invariant: cannot index an evicted balance history archive")
	}
	if entry.index != nil {
		return nil
	}
	if uint64(len(index.offsets)) > (^uint64(0)>>1)/8 {
		return errors.New("balance history archive index exceeds int64 size")
	}

	entry.index = index
	entry.indexBytes = int64(len(index.offsets)) * 8
	c.bytes += entry.indexBytes

	return c.evictLocked()
}

func (c *cache) newLeaseLocked(entry *cacheEntry) *Lease {
	return &Lease{cache: c, entry: entry, ref: entry.ref, path: entry.path}
}

func (c *cache) stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := CacheStats{Bytes: c.bytes, Entries: len(c.entries)}
	for _, entry := range c.entries {
		stats.Leases += entry.leases
	}

	return stats
}

func fsyncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		closeErr := directory.Close()
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}

		return err
	}

	return directory.Close()
}

const sha256Length = 32

// Lease pins one cached run against eviction.
type Lease struct {
	mu    sync.Mutex
	cache *cache
	entry *cacheEntry
	ref   Ref
	path  string
	done  bool
}

// Open verifies the pinned file immediately before exposing records.
// The lease must outlive the returned Reader.
func (l *Lease) Open() (*Reader, error) {
	l.mu.Lock()
	if l.done {
		l.mu.Unlock()

		return nil, errors.New("balance history archive lease is closed")
	}
	reader, err := OpenFile(l.path, l.ref)
	l.mu.Unlock()
	if err != nil {
		invalidateErr := l.invalidate()
		if invalidateErr != nil {
			err = errors.Join(err, invalidateErr)
		}

		return nil, err
	}

	return reader, nil
}

// OpenIndexed verifies the pinned blob and returns a disk-backed reader with
// logarithmic key seeks. The immutable offset index is shared by every reader
// opened from leases on the same cache entry. The lease must outlive the
// returned reader.
func (l *Lease) OpenIndexed() (*IndexedReader, error) {
	l.mu.Lock()
	if l.done {
		l.mu.Unlock()

		return nil, errors.New("balance history archive lease is closed")
	}

	entry := l.entry
	entry.indexMu.Lock()
	index := entry.index
	var err error
	if index == nil {
		index, err = buildRecordIndex(l.path, l.ref)
		if err == nil {
			err = l.cache.installIndex(entry, index)
		}
	} else {
		err = verifyFile(l.path, l.ref)
	}
	var reader *IndexedReader
	if err == nil {
		reader, err = openIndexedReader(l.path, l.ref, index)
	}
	entry.indexMu.Unlock()
	l.mu.Unlock()
	if err == nil {
		return reader, nil
	}

	invalidateErr := l.invalidate()
	if invalidateErr != nil {
		err = errors.Join(err, invalidateErr)
	}

	return nil, err
}

// Close releases the eviction pin. It is idempotent.
func (l *Lease) Close() error {
	l.mu.Lock()
	if l.done {
		l.mu.Unlock()

		return nil
	}
	l.done = true
	cache := l.cache
	entry := l.entry
	l.mu.Unlock()

	cache.mu.Lock()
	defer cache.mu.Unlock()

	if entry.leases <= 0 {
		return errors.New("invariant: balance history archive lease underflow")
	}
	entry.leases--
	if entry.invalid && entry.leases == 0 {
		return cache.removeLocked(entry)
	}

	return cache.evictLocked()
}

func (l *Lease) invalidate() error {
	l.mu.Lock()
	if l.done {
		l.mu.Unlock()

		return nil
	}
	l.done = true
	cache := l.cache
	entry := l.entry
	l.mu.Unlock()

	cache.mu.Lock()
	defer cache.mu.Unlock()

	entry.invalid = true
	if entry.leases <= 0 {
		return errors.New("invariant: balance history archive lease underflow")
	}
	entry.leases--
	if entry.leases == 0 {
		return cache.removeLocked(entry)
	}

	return nil
}
