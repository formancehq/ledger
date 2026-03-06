package state

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// SealRequest contains the data needed to seal a period.
type SealRequest struct {
	PeriodID       uint64
	CloseSequence  uint64
	LastLogHash    []byte
	CheckpointPath string
}

// SealProposer is a callback to propose a SealPeriod order back into Raft.
type SealProposer func(periodID uint64, sealingHash []byte)

// SealerPeriodState provides the Sealer with read access to the current period
// state. Implemented by *Machine.
type SealerPeriodState interface {
	ClosingPeriod() *commonpb.Period
}

// Sealer runs in the background to compute sealing hashes for closing periods.
// It reads seal requests from sealRequestCh (fed by the Machine on ClosePeriod
// or directly by crash-recovery logic in the Sealer/Node).
// Only the leader node computes the hash and proposes SealPeriod. Followers
// poll until the period is no longer CLOSING (sealed by the leader through Raft).
type Sealer struct {
	logger        logging.Logger
	dataStore     *dal.Store
	sealRequestCh chan SealRequest
	proposeFn     SealProposer
	isLeader      func() bool
	periodState   SealerPeriodState
	w             worker.Worker
}

// NewSealer creates a new background sealer.
func NewSealer(
	logger logging.Logger,
	dataStore *dal.Store,
	sealRequestCh chan SealRequest,
	proposeFn SealProposer,
	isLeader func() bool,
	periodState SealerPeriodState,
) *Sealer {
	return &Sealer{
		logger:        logger.WithFields(map[string]any{"cmp": "sealer"}),
		dataStore:     dataStore,
		sealRequestCh: sealRequestCh,
		proposeFn:     proposeFn,
		isLeader:      isLeader,
		periodState:   periodState,
		w:             worker.New(),
	}
}

// Start launches the background sealing goroutine and recovers any
// pending seal from a previous crash.
func (s *Sealer) Start() {
	s.w.Run(func(stop <-chan struct{}) {
		worker.DrainChannel(stop, s.sealRequestCh, func(req SealRequest) {
			worker.RetryWithBackoff(stop, s.logger, func() error {
				return s.seal(req)
			})
		})
	})

	s.recoverPendingSeal()
}

// recoverPendingSeal checks for a pending seal from a previous crash and
// re-enqueues it if a seal checkpoint exists on disk.
func (s *Sealer) recoverPendingSeal() {
	closingPeriod := s.periodState.ClosingPeriod()
	if closingPeriod == nil {
		return
	}

	checkpointPath, exists := s.dataStore.TemporaryCheckpointPath("seal")
	if !exists {
		return
	}

	req := SealRequestFromPeriod(closingPeriod)
	req.CheckpointPath = checkpointPath
	s.logger.WithFields(map[string]any{
		"periodId":      req.PeriodID,
		"closeSequence": req.CloseSequence,
	}).Infof("Recovering pending period seal after restart")

	select {
	case s.sealRequestCh <- *req:
	default:
	}
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (s *Sealer) Stop() {
	s.w.Stop()
}

// seal computes the sealing hash for a period and proposes a SealPeriod order.
//
// The flow handles both leader and follower nodes:
//   - First, check if the period is still in CLOSING state. If not, the seal
//     was already applied through Raft — exit silently.
//   - If the period is still CLOSING and this node is not the leader, return
//     worker.ErrNotLeader so the retry loop waits and re-checks.
//   - Only the leader computes the hash and proposes SealPeriod.
//
// sealing_hash = BLAKE3(period_id || close_sequence || last_log_hash || state_hash)
// state_hash = BLAKE3(all attribute key+value pairs in the checkpoint).
func (s *Sealer) seal(req SealRequest) error {
	logFields := map[string]any{
		"periodId":      req.PeriodID,
		"closeSequence": req.CloseSequence,
	}

	// Check if the period is still CLOSING — if not, the leader already sealed
	// it and the SealPeriod was applied through Raft. Followers can exit.
	cp := s.periodState.ClosingPeriod()
	if cp == nil || cp.GetId() != req.PeriodID {
		s.logger.WithFields(logFields).Infof("Period no longer closing (sealed by leader), done")
		// Clean up the seal checkpoint if it exists on this node
		_ = s.dataStore.RemoveTemporaryCheckpoint("seal")

		return nil
	}

	// Period is still CLOSING — only the leader should compute the hash.
	if !s.isLeader() {
		return worker.ErrNotLeader
	}

	s.logger.WithFields(logFields).Infof("Starting period sealing")

	// Open the seal checkpoint as a read-only Pebble DB
	db, err := pebble.Open(req.CheckpointPath, &pebble.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("opening seal checkpoint: %w", err)
	}

	// Compute state hash by iterating all attribute entries in the checkpoint
	stateHash, err := computeStateHash(db)
	// Close the DB before cleanup — must happen regardless of hash computation result
	_ = db.Close()

	if err != nil {
		return err
	}

	// Clean up the seal checkpoint now that the hash has been computed.
	// On crash recovery, the checkpoint will be re-created from WAL replay.
	_ = s.dataStore.RemoveTemporaryCheckpoint("seal")

	// Compute sealing hash
	hasher := blake3.New()
	buf := make([]byte, 8)

	binary.BigEndian.PutUint64(buf, req.PeriodID)
	_, _ = hasher.Write(buf)

	binary.BigEndian.PutUint64(buf, req.CloseSequence)
	_, _ = hasher.Write(buf)

	if len(req.LastLogHash) > 0 {
		_, _ = hasher.Write(req.LastLogHash)
	}

	_, _ = hasher.Write(stateHash)

	sealingHash := hasher.Sum(nil)

	s.logger.WithFields(map[string]any{
		"periodId":    req.PeriodID,
		"sealingHash": sealingHash,
	}).Infof("Period sealing complete, proposing SealPeriod")

	// Propose the SealPeriod order back into Raft
	s.proposeFn(req.PeriodID, sealingHash)

	return nil
}

// iteratorSource abstracts anything that can create a Pebble iterator.
type iteratorSource interface {
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

// computeStateHash iterates all raw attribute entries in [0xF1, 0xF2)
// and hashes every key+value pair.
//
// The checkpoint is frozen at the exact ClosePeriod point, so all entries
// in it belong to the period being sealed — no filtering is needed.
//
// This is deterministic because Pebble iteration order is deterministic
// and compaction is 100% deterministic via Raft.
func computeStateHash(src iteratorSource) ([]byte, error) {
	hasher := blake3.New()

	iter, err := src.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneAttributesStart},
		UpperBound: []byte{dal.ZoneAttributesEnd},
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for state hash: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		_, _ = hasher.Write(iter.Key())

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading attribute value: %w", err)
		}

		_, _ = hasher.Write(value)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating attributes: %w", err)
	}

	return hasher.Sum(nil), nil
}
