package state

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
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

// compactionRequest carries the data needed for a single background compaction run.
type compactionRequest struct {
	compactionIndex uint64
	dirtyKeys       map[string]struct{}
}

// Compactor runs background compaction on volume diffs to reduce storage.
//
// It receives tracked dirty keys from the Machine at each generation rotation
// and performs only Pebble DeleteRange writes (no reads). This eliminates the
// expensive List() and ScanEntries() calls that previously blocked the system.
type Compactor struct {
	logger    logging.Logger
	attrs     *attributes.Attributes
	dataStore *data.Store
	batchSize int
	compactCh chan compactionRequest
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
		attrs:                attributes.New(),
		batchSize:            batchSize,
		compactCh:            make(chan compactionRequest, 1),
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

// Signal sends a compaction request with dirty keys (non-blocking).
// If a request is already pending, the new one is dropped (the inline
// compaction in ApplyEntries already handled this generation).
func (c *Compactor) Signal(compactionIndex uint64, dirtyKeys map[string]struct{}) {
	req := compactionRequest{
		compactionIndex: compactionIndex,
		dirtyKeys:       dirtyKeys,
	}
	select {
	case c.compactCh <- req:
	default:
		// Already has a pending request, skip
	}
}

func (c *Compactor) run() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.stopCh:
			return
		case req := <-c.compactCh:
			if err := c.compact(req); err != nil {
				c.logger.Errorf("Background compaction failed: %v", err)
			}
		}
	}
}

func (c *Compactor) compact(req compactionRequest) error {
	c.logger.WithFields(map[string]any{
		"compactionIndex": req.compactionIndex,
		"dirtyKeys":       len(req.dirtyKeys),
	}).Debugf("Starting background compaction")

	batch := c.dataStore.NewBatch()
	defer func() { _ = batch.Cancel() }()

	var batchCount int
	for keyStr := range req.dirtyKeys {
		canonicalKey := []byte(keyStr)
		if err := c.attrs.Input.DeleteOldest(batch, req.compactionIndex, canonicalKey); err != nil {
			return fmt.Errorf("compacting input volume: %w", err)
		}
		if err := c.attrs.Output.DeleteOldest(batch, req.compactionIndex, canonicalKey); err != nil {
			return fmt.Errorf("compacting output volume: %w", err)
		}
		batchCount++
		if batchCount >= c.batchSize {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("committing batch: %w", err)
			}
			c.compactedKeysCounter.Add(context.Background(), int64(batchCount))
			batch = c.dataStore.NewBatch()
			batchCount = 0
		}
	}

	if batchCount > 0 {
		if err := batch.Commit(); err != nil {
			return fmt.Errorf("committing final batch: %w", err)
		}
		c.compactedKeysCounter.Add(context.Background(), int64(batchCount))
	}

	c.logger.WithFields(map[string]any{
		"compactedKeys": len(req.dirtyKeys),
	}).Debugf("Background compaction complete")

	return nil
}
