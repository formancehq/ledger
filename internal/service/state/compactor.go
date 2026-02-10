package state

import (
	"context"
	"fmt"
	"math"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"go.opentelemetry.io/otel/metric"
)

// CompactorConfig holds configuration for the background volume diff compactor.
type CompactorConfig struct {
	Enabled   bool
	BatchSize int // Keys per Pebble batch (default: 100)
}

// DefaultCompactorConfig returns the default compactor configuration.
func DefaultCompactorConfig() CompactorConfig {
	return CompactorConfig{
		Enabled:   true,
		BatchSize: 100,
	}
}

// Compactor runs background compaction on volume diffs to reduce storage.
//
// Phase 1 - Intermediate Diff Removal:
// For each volume key, keep only the latest base + latest diff, delete everything else.
// This is always safe because intermediate cumulative diffs are redundant.
//
// Phase 2 - Cold Key Consolidation:
// For keys NOT in cache that have a base entry, consolidate base+diff into a single base.
// Cold keys won't receive new cumulative diffs, so consolidation is safe.
type Compactor struct {
	logger    logging.Logger
	dataStore *data.Store
	cache     *cache.Cache
	attrs     *attributes.Attributes // Own instance for thread-safe writes
	hasher    *attributes.KeyHasher  // For canonical→U128 lookups
	batchSize int
	compactCh chan struct{}
	stopCh    chan struct{}
	doneCh    chan struct{}

	// Metrics
	compactedKeysCounter metric.Int64Counter
	compactionDuration   metric.Float64Histogram
}

// NewCompactor creates a new background volume diff compactor.
func NewCompactor(
	logger logging.Logger,
	dataStore *data.Store,
	cache *cache.Cache,
	meter metric.Meter,
	cfg CompactorConfig,
) (*Compactor, error) {
	compactedKeys, err := meter.Int64Counter(
		"compactor.compacted_keys",
		metric.WithDescription("Total number of keys compacted by the background compactor"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating compacted_keys counter: %w", err)
	}

	compactionDuration, err := meter.Float64Histogram(
		"compactor.duration",
		metric.WithDescription("Duration of background compaction runs"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating compaction_duration histogram: %w", err)
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	return &Compactor{
		logger:               logger.WithFields(map[string]any{"cmp": "compactor"}),
		dataStore:            dataStore,
		cache:                cache,
		attrs:                attributes.New(), // Own instance for thread-safe writes
		hasher:               attributes.NewKeyHasher(attributes.DefaultKeys),
		batchSize:            batchSize,
		compactCh:            make(chan struct{}, 1),
		stopCh:               make(chan struct{}),
		doneCh:               make(chan struct{}),
		compactedKeysCounter: compactedKeys,
		compactionDuration:   compactionDuration,
	}, nil
}

// Start launches the background compaction goroutine.
func (c *Compactor) Start() {
	go c.run()
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (c *Compactor) Stop() {
	close(c.stopCh)
	<-c.doneCh
}

// Signal triggers a background compaction (non-blocking).
func (c *Compactor) Signal() {
	select {
	case c.compactCh <- struct{}{}:
	default:
		// Already signaled, skip
	}
}

func (c *Compactor) run() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.stopCh:
			return
		case <-c.compactCh:
			if err := c.compact(); err != nil {
				c.logger.Errorf("Background compaction failed: %v", err)
			}
		}
	}
}

func (c *Compactor) compact() error {
	c.logger.Debugf("Starting background compaction")

	var totalCompacted int64

	n, err := c.compactAttribute(c.attrs.Input, c.cache.Input)
	if err != nil {
		return fmt.Errorf("compacting input volumes: %w", err)
	}
	totalCompacted += n

	n, err = c.compactAttribute(c.attrs.Output, c.cache.Output)
	if err != nil {
		return fmt.Errorf("compacting output volumes: %w", err)
	}
	totalCompacted += n

	c.logger.WithFields(map[string]any{
		"compactedKeys": totalCompacted,
	}).Debugf("Background compaction complete")

	return nil
}

// compactAttribute runs Phase 1 and Phase 2 for a single attribute type.
func (c *Compactor) compactAttribute(
	attr *attributes.Attribute[*commonpb.BigInt],
	cacheAttr *cache.AttributeCache[*raftcmdpb.VolumeHolder],
) (int64, error) {
	entries, err := attr.List(c.dataStore)
	if err != nil {
		return 0, fmt.Errorf("listing attribute keys: %w", err)
	}

	var (
		compactedKeys int64
		batchCount    int
		batch         = c.dataStore.NewBatch()
	)
	defer func() {
		_ = batch.Cancel()
	}()

	for _, entry := range entries {
		scan, err := attr.ScanEntries(c.dataStore, entry.CanonicalKey)
		if err != nil {
			return compactedKeys, fmt.Errorf("scanning entries: %w", err)
		}

		compacted, err := c.compactKey(batch, attr, cacheAttr, entry.CanonicalKey, scan)
		if err != nil {
			return compactedKeys, fmt.Errorf("compacting key: %w", err)
		}
		if compacted {
			compactedKeys++
			batchCount++
		}

		// Commit batch periodically
		if batchCount >= c.batchSize {
			if err := batch.Commit(); err != nil {
				return compactedKeys, fmt.Errorf("committing batch: %w", err)
			}
			c.compactedKeysCounter.Add(context.Background(), compactedKeys)
			batch = c.dataStore.NewBatch()
			batchCount = 0
		}
	}

	// Commit remaining
	if batchCount > 0 {
		if err := batch.Commit(); err != nil {
			return compactedKeys, fmt.Errorf("committing final batch: %w", err)
		}
		c.compactedKeysCounter.Add(context.Background(), compactedKeys)
	}

	return compactedKeys, nil
}

// compactKey applies Phase 1 and Phase 2 to a single key.
// Returns true if the key was compacted.
func (c *Compactor) compactKey(
	batch *data.Batch,
	attr *attributes.Attribute[*commonpb.BigInt],
	cacheAttr *cache.AttributeCache[*raftcmdpb.VolumeHolder],
	canonicalKey []byte,
	scan *attributes.ScanResult[*commonpb.BigInt],
) (bool, error) {
	// Phase 1: Intermediate Diff Removal
	// Skip if 2 or fewer entries (already minimal)
	if scan.TotalEntries <= 2 {
		// Phase 2 opportunity: if exactly 2 entries (base + diff), check cold key consolidation
		if scan.TotalEntries == 2 && scan.HasBase && scan.HasDiff {
			return c.consolidateColdKey(batch, attr, cacheAttr, canonicalKey, scan)
		}
		return false, nil
	}

	// Phase 1: Delete everything before the latest diff, then re-write the base
	if scan.HasDiff {
		// Delete entries before the latest diff index (removes old base + intermediate diffs)
		if err := attr.DeleteOldest(batch, scan.LatestDiffIndex, canonicalKey); err != nil {
			return false, fmt.Errorf("deleting oldest entries: %w", err)
		}

		// Re-write the base at its original index (it was deleted by DeleteOldest)
		if scan.HasBase && scan.LatestBaseIndex < scan.LatestDiffIndex {
			if err := attr.SetBase(batch, scan.LatestBaseIndex, canonicalKey, scan.LatestBase); err != nil {
				return false, fmt.Errorf("re-writing base: %w", err)
			}
		}

		// Now try Phase 2 on the reduced key
		if scan.HasBase {
			return c.consolidateColdKey(batch, attr, cacheAttr, canonicalKey, scan)
		}

		return true, nil
	}

	// Only base entries, no diffs - nothing to compact
	return false, nil
}

// consolidateColdKey checks if a key is cold (not in cache) and consolidates base+diff
// into a single base entry (Phase 2).
func (c *Compactor) consolidateColdKey(
	batch *data.Batch,
	attr *attributes.Attribute[*commonpb.BigInt],
	cacheAttr *cache.AttributeCache[*raftcmdpb.VolumeHolder],
	canonicalKey []byte,
	scan *attributes.ScanResult[*commonpb.BigInt],
) (bool, error) {
	if !scan.HasBase || !scan.HasDiff {
		return false, nil
	}

	// Check if key is in cache (hot key) - skip if so
	u128, _ := c.hasher.MakeKey(canonicalKey)
	if _, ok := cacheAttr.Get(u128); ok {
		return false, nil // Hot key, don't consolidate
	}

	// Compute the consolidated value
	consolidated, err := attr.ComputeValue(c.dataStore, math.MaxUint64, canonicalKey)
	if err != nil {
		return false, fmt.Errorf("computing consolidated value: %w", err)
	}

	// Delete all entries for this key
	latestIndex := scan.LatestDiffIndex
	if scan.LatestBaseIndex > latestIndex {
		latestIndex = scan.LatestBaseIndex
	}
	if err := attr.Delete(batch, canonicalKey); err != nil {
		return false, fmt.Errorf("deleting all entries: %w", err)
	}

	// Write a single consolidated base at the latest index
	if err := attr.SetBase(batch, latestIndex, canonicalKey, consolidated); err != nil {
		return false, fmt.Errorf("writing consolidated base: %w", err)
	}

	return true, nil
}
