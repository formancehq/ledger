package state

import (
	"context"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const maxEvictionBatchSize = 10000

// IdempotencyEvictionScheduler periodically proposes an IdempotencyEviction
// command through Raft when this node is the leader. The cutoff timestamp
// is computed from wall-clock time minus the configured TTL and embedded in
// the Raft proposal so all nodes apply the same deterministic eviction.
//
// The scheduler pre-scans the Pebble time index on the leader side to collect
// expired key hashes (up to maxEvictionBatchSize per tick). These hashes are
// included in the proposal so the FSM apply path is write-only (no Pebble reads).
//
// proposeFn receives a context derived from the scheduler's stop signal —
// callers should propagate it to their Raft propose so a Stop() cancels an
// in-flight proposal cleanly. There is intentionally no internal timeout: a
// timeout that fires after Raft has accepted the proposal would cause the
// next tick to re-submit the same hashes, and the FSM apply path's
// SingleDelete contract forbids double-deleting a Pebble main key.
type IdempotencyEvictionScheduler struct {
	logger      logging.Logger
	isLeader    func() bool
	proposeFn   func(ctx context.Context, cutoffMicros uint64, lastScannedTimeIndexKey []byte, pebbleKeyHashes [][]byte)
	store       dal.BackgroundScanner
	idempotency *IdempotencyStore
	interval    time.Duration
	ttl         time.Duration
	w           worker.Worker
}

// NewIdempotencyEvictionScheduler creates a new scheduler.
// proposeFn is called with a stop-cancelled context, the cutoff timestamp,
// the full Pebble time-index key of the last scanned entry (used as the
// exact DeleteRange upper bound), and the pre-scanned key hashes to submit
// via Raft. The ctx is cancelled when Stop() is invoked, which is the
// scheduler's sole shutdown signal.
func NewIdempotencyEvictionScheduler(
	logger logging.Logger,
	isLeader func() bool,
	proposeFn func(ctx context.Context, cutoffMicros uint64, lastScannedTimeIndexKey []byte, pebbleKeyHashes [][]byte),
	store dal.BackgroundScanner,
	idempotency *IdempotencyStore,
	interval time.Duration,
	ttl time.Duration,
) *IdempotencyEvictionScheduler {
	return &IdempotencyEvictionScheduler{
		logger:      logger,
		isLeader:    isLeader,
		proposeFn:   proposeFn,
		store:       store,
		idempotency: idempotency,
		interval:    interval,
		ttl:         ttl,
		w:           worker.New(),
	}
}

// Start begins the eviction loop in a background goroutine.
func (s *IdempotencyEvictionScheduler) Start() {
	s.w.RunCtx(s.loop)
}

// Stop signals the eviction loop to stop and waits for it to finish.
func (s *IdempotencyEvictionScheduler) Stop() {
	s.w.Stop()
}

func (s *IdempotencyEvictionScheduler) loop(ctx context.Context) {
	// ctx is supplied by Worker.RunCtx and is cancelled by Stop(). It
	// flows to proposeFn so an in-flight Raft propose unblocks on
	// shutdown instead of pinning the worker, and so no bounded timeout
	// can fire after Raft has accepted the proposal (which would force
	// a retry that double-SingleDeletes the same Pebble main keys —
	// undefined per Pebble's SingleDelete contract).
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isLeader() {
				continue
			}

			cutoff := uint64(time.Now().UnixMicro()) - uint64(s.ttl.Microseconds())

			// Pre-scan Pebble time index on the leader to collect expired key hashes.
			// The hashes are included in the Raft proposal so the FSM apply is write-only.
			// Batching is bounded by maxEvictionBatchSize to avoid oversized Raft commands.
			handle, err := s.store.NewDirectReadHandle()
			if err != nil {
				s.logger.Errorf("Failed to open read handle for idempotency eviction scan: %v", err)

				continue
			}

			hashes, lastScannedKey, err := s.idempotency.ScanExpiredKeyHashes(handle, cutoff, maxEvictionBatchSize)
			_ = handle.Close()

			if err != nil {
				s.logger.Errorf("Failed to scan expired idempotency keys: %v", err)

				continue
			}

			if len(hashes) == 0 {
				continue
			}

			s.logger.Debugf("Proposing idempotency eviction with cutoff=%d, pebbleKeys=%d", cutoff, len(hashes))
			s.proposeFn(ctx, cutoff, lastScannedKey, hashes)
		}
	}
}
