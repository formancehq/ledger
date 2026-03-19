package state

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
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
type SealProposer func(periodID uint64, sealingHash, stateHash []byte)

// SealerPeriodState provides the Sealer with read access to the current period
// state. Implemented by *Machine.
type SealerPeriodState interface {
	ClosingPeriods() []*commonpb.Period
	ClosingPeriodByID(id uint64) (*commonpb.Period, bool)
}

// SealCheckpointName returns the checkpoint name for a given period ID.
func SealCheckpointName(periodID uint64) string {
	return fmt.Sprintf("seal-%d", periodID)
}

// Sealer runs in the background to compute sealing hashes for closing periods.
// It reads seal requests from sealRequestCh (fed by the Machine on ClosePeriod
// or directly by crash-recovery logic in the Sealer/Node).
// Only the leader node computes the hash and proposes SealPeriod. Followers
// poll until the period is no longer CLOSING (sealed by the leader through Raft).
type Sealer struct {
	logger        logging.Logger
	dataStore     *dal.Store
	attrs         *attributes.Attributes
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
	attrs *attributes.Attributes,
	sealRequestCh chan SealRequest,
	proposeFn SealProposer,
	isLeader func() bool,
	periodState SealerPeriodState,
) *Sealer {
	return &Sealer{
		logger:        logger.WithFields(map[string]any{"cmp": "sealer"}),
		dataStore:     dataStore,
		attrs:         attrs,
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

// recoverPendingSeal checks for pending seals from a previous crash and
// re-enqueues them if seal checkpoints exist on disk.
func (s *Sealer) recoverPendingSeal() {
	for _, cp := range s.periodState.ClosingPeriods() {
		name := SealCheckpointName(cp.GetId())

		checkpointPath, exists := s.dataStore.TemporaryCheckpointPath(name)
		if !exists {
			continue
		}

		req := SealRequestFromPeriod(cp)
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

	checkpointName := SealCheckpointName(req.PeriodID)

	// Check if the period is still CLOSING — if not, the leader already sealed
	// it and the SealPeriod was applied through Raft. Followers can exit.
	if _, ok := s.periodState.ClosingPeriodByID(req.PeriodID); !ok {
		s.logger.WithFields(logFields).Infof("Period no longer closing (sealed by leader), done")
		// Clean up the seal checkpoint if it exists on this node
		_ = s.dataStore.RemoveTemporaryCheckpoint(checkpointName)

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

	// Compute state hash by iterating computed attribute values in the checkpoint
	stateHash, err := computeStateHash(db, s.attrs)
	// Close the DB before cleanup — must happen regardless of hash computation result
	_ = db.Close()

	if err != nil {
		return err
	}

	// Clean up the seal checkpoint now that the hash has been computed.
	// On crash recovery, the checkpoint will be re-created from WAL replay.
	_ = s.dataStore.RemoveTemporaryCheckpoint(checkpointName)

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
	s.proposeFn(req.PeriodID, sealingHash, stateHash)

	return nil
}

// ComputeStateHash hashes computed attribute values (last value per canonical key)
// for Volumes, Metadata, and TransactionState.
//
// The checkpoint is frozen at the exact ClosePeriod point, so all entries
// in it belong to the period being sealed — no filtering is needed.
//
// This is deterministic because Pebble iteration order is deterministic
// and compaction is 100% deterministic via Raft.
//
// Exported so the checker can reuse the same algorithm to verify state_hash.
func ComputeStateHash(reader dal.PebbleReader, attrs *attributes.Attributes) ([]byte, error) {
	return computeStateHash(reader, attrs)
}

func computeStateHash(reader dal.PebbleReader, attrs *attributes.Attributes) ([]byte, error) {
	hasher := blake3.New()

	// Hash volumes (deterministic Pebble key order)
	volIter, err := attrs.Volume.NewStreamingIter(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("creating volume iterator: %w", err)
	}

	for volIter.Next() {
		e := volIter.Entry()

		_, _ = hasher.Write(e.CanonicalKey)

		data, marshalErr := e.Value.MarshalVT()
		if marshalErr != nil {
			_ = volIter.Close()

			return nil, fmt.Errorf("marshaling volume: %w", marshalErr)
		}

		_, _ = hasher.Write(data)
	}

	if closeErr := volIter.Close(); closeErr != nil {
		return nil, fmt.Errorf("closing volume iterator: %w", closeErr)
	}

	if err := volIter.Err(); err != nil {
		return nil, fmt.Errorf("hashing volumes: %w", err)
	}

	// Hash metadata
	metaIter, err := attrs.Metadata.NewStreamingIter(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("creating metadata iterator: %w", err)
	}

	for metaIter.Next() {
		e := metaIter.Entry()

		_, _ = hasher.Write(e.CanonicalKey)

		data, marshalErr := e.Value.MarshalVT()
		if marshalErr != nil {
			_ = metaIter.Close()

			return nil, fmt.Errorf("marshaling metadata: %w", marshalErr)
		}

		_, _ = hasher.Write(data)
	}

	if closeErr := metaIter.Close(); closeErr != nil {
		return nil, fmt.Errorf("closing metadata iterator: %w", closeErr)
	}

	if err := metaIter.Err(); err != nil {
		return nil, fmt.Errorf("hashing metadata: %w", err)
	}

	// Hash transaction states
	txIter, err := attrs.Transaction.NewStreamingIter(reader, nil)
	if err != nil {
		return nil, fmt.Errorf("creating transaction iterator: %w", err)
	}

	for txIter.Next() {
		e := txIter.Entry()

		_, _ = hasher.Write(e.CanonicalKey)

		data, marshalErr := e.Value.MarshalVT()
		if marshalErr != nil {
			_ = txIter.Close()

			return nil, fmt.Errorf("marshaling transaction state: %w", marshalErr)
		}

		_, _ = hasher.Write(data)
	}

	if closeErr := txIter.Close(); closeErr != nil {
		return nil, fmt.Errorf("closing transaction iterator: %w", closeErr)
	}

	if err := txIter.Err(); err != nil {
		return nil, fmt.Errorf("hashing transaction states: %w", err)
	}

	return hasher.Sum(nil), nil
}
