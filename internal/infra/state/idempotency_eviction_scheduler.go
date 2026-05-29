package state

import (
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

// IdempotencyEvictionScheduler periodically proposes an IdempotencyEviction
// command through Raft when this node is the leader. The cutoff timestamp
// is computed from wall-clock time minus the configured TTL and embedded in
// the Raft proposal so all nodes apply the same deterministic eviction.
type IdempotencyEvictionScheduler struct {
	logger    logging.Logger
	isLeader  func() bool
	proposeFn func(cutoffMicros uint64)
	interval  time.Duration
	ttl       time.Duration
	w         worker.Worker
}

// NewIdempotencyEvictionScheduler creates a new scheduler.
// proposeFn is called with the cutoff timestamp to submit via Raft.
func NewIdempotencyEvictionScheduler(
	logger logging.Logger,
	isLeader func() bool,
	proposeFn func(cutoffMicros uint64),
	interval time.Duration,
	ttl time.Duration,
) *IdempotencyEvictionScheduler {
	return &IdempotencyEvictionScheduler{
		logger:    logger,
		isLeader:  isLeader,
		proposeFn: proposeFn,
		interval:  interval,
		ttl:       ttl,
		w:         worker.New(),
	}
}

// Start begins the eviction loop in a background goroutine.
func (s *IdempotencyEvictionScheduler) Start() {
	s.w.Run(s.loop)
}

// Stop signals the eviction loop to stop and waits for it to finish.
func (s *IdempotencyEvictionScheduler) Stop() {
	s.w.Stop()
}

func (s *IdempotencyEvictionScheduler) loop(stop <-chan struct{}) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if !s.isLeader() {
				continue
			}

			cutoff := uint64(time.Now().UnixMicro()) - uint64(s.ttl.Microseconds())
			s.logger.Debugf("Proposing idempotency eviction with cutoff=%d", cutoff)
			s.proposeFn(cutoff)
		}
	}
}
