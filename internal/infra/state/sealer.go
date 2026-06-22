package state

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// SealRequest contains the data needed to seal a chapter.
type SealRequest struct {
	ChapterID      uint64
	CloseSequence  uint64
	LastAuditHash  []byte
	CheckpointPath string
}

// SealProposer is a callback to propose a SealChapter order back into Raft.
type SealProposer func(chapterID uint64, sealingHash, stateHash []byte) error

// SealerChapterState provides the Sealer with read access to the current chapter
// state. Implemented by *Machine.
type SealerChapterState interface {
	ClosingChapters() []*commonpb.Chapter
	ClosingChapterByID(id uint64) (*commonpb.Chapter, bool)
}

// SealCheckpointName returns the checkpoint name for a given chapter ID.
func SealCheckpointName(chapterID uint64) string {
	return fmt.Sprintf("seal-%d", chapterID)
}

// Sealer runs in the background to compute sealing hashes for closing chapters.
// It reads seal requests from sealRequestCh (fed by the Machine on CloseChapter
// or directly by crash-recovery logic in the Sealer/Node).
// Only the leader node computes the hash and proposes SealChapter. Followers
// poll until the chapter is no longer CLOSING (sealed by the leader through Raft).
type Sealer struct {
	logger            logging.Logger
	dataStore         dal.CheckpointStaging
	attrs             *attributes.Attributes
	sealRequestCh     *worker.Channel[SealRequest]
	proposeFn         SealProposer
	isLeader          func() bool
	chapterState      SealerChapterState
	reconcileInterval time.Duration
	w                 worker.Worker
}

// NewSealer creates a new background sealer.
func NewSealer(
	logger logging.Logger,
	dataStore dal.CheckpointStaging,
	attrs *attributes.Attributes,
	sealRequestCh *worker.Channel[SealRequest],
	proposeFn SealProposer,
	isLeader func() bool,
	chapterState SealerChapterState,
) *Sealer {
	return &Sealer{
		logger:            logger.WithFields(map[string]any{"cmp": "sealer"}),
		dataStore:         dataStore,
		attrs:             attrs,
		sealRequestCh:     sealRequestCh,
		proposeFn:         proposeFn,
		isLeader:          isLeader,
		chapterState:      chapterState,
		reconcileInterval: sealReconcileInterval,
		w:                 worker.New(),
	}
}

// reconcileInterval is the interval at which the Sealer re-checks for
// pending seals that may have been missed due to dropped channel signals.
const sealReconcileInterval = 30 * time.Second

// Start launches the background sealing goroutine, recovers any pending
// seal from a previous crash, and starts periodic reconciliation.
func (s *Sealer) Start() {
	s.w.Run(func(stop <-chan struct{}) {
		// Recover on start and periodically in a background goroutine.
		// Recovery uses blocking sends, so it must run concurrently with
		// the drain loop to avoid deadlocking when the channel is full.
		go func() {
			s.recoverPendingSeal(stop)

			worker.RunTicker(stop, s.reconcileInterval, func() {
				s.recoverPendingSeal(stop)
			})
		}()

		// Main drain loop.
		worker.DrainChannel(stop, s.sealRequestCh.Receive(), func(req SealRequest) {
			worker.RetryWithBackoff(stop, s.logger, func() error {
				return s.seal(req)
			})
		})
	})
}

// recoverPendingSeal checks for pending seals from a previous crash and
// re-enqueues them if seal checkpoints exist on disk.
// Sends block until the worker drains or stop is closed.
func (s *Sealer) recoverPendingSeal(stop <-chan struct{}) {
	for _, cp := range s.chapterState.ClosingChapters() {
		name := SealCheckpointName(cp.GetId())

		checkpointPath, exists := s.dataStore.TemporaryCheckpointPath(name)
		if !exists {
			continue
		}

		req := SealRequestFromChapter(cp)
		req.CheckpointPath = checkpointPath
		s.logger.WithFields(map[string]any{
			"chapterId":     req.ChapterID,
			"closeSequence": req.CloseSequence,
		}).Infof("Recovering pending chapter seal")

		if !s.sealRequestCh.Send(*req, stop) {
			return
		}
	}
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (s *Sealer) Stop() {
	s.w.Stop()
}

// seal computes the sealing hash for a chapter and proposes a SealChapter order.
//
// The flow handles both leader and follower nodes:
//   - First, check if the chapter is still in CLOSING state. If not, the seal
//     was already applied through Raft — exit silently.
//   - If the chapter is still CLOSING and this node is not the leader, return
//     worker.ErrNotLeader so the retry loop waits and re-checks.
//   - Only the leader computes the hash and proposes SealChapter.
//
// sealing_hash = BLAKE3(chapter_id || close_sequence || last_log_hash || state_hash)
// state_hash = BLAKE3(all attribute key+value pairs in the checkpoint).
func (s *Sealer) seal(req SealRequest) error {
	logFields := map[string]any{
		"chapterId":     req.ChapterID,
		"closeSequence": req.CloseSequence,
	}

	checkpointName := SealCheckpointName(req.ChapterID)

	// Check if the chapter is still CLOSING — if not, the leader already sealed
	// it and the SealChapter was applied through Raft. Followers can exit.
	if _, ok := s.chapterState.ClosingChapterByID(req.ChapterID); !ok {
		s.logger.WithFields(logFields).Infof("Chapter no longer closing (sealed by leader), done")
		// Clean up the seal checkpoint if it exists on this node
		_ = s.dataStore.RemoveTemporaryCheckpoint(checkpointName)

		return nil
	}

	// Chapter is still CLOSING — only the leader should compute the hash.
	if !s.isLeader() {
		return worker.ErrNotLeader
	}

	s.logger.WithFields(logFields).Infof("Starting chapter sealing")

	// Open the seal checkpoint as a read-only Pebble DB
	db, err := pebble.Open(req.CheckpointPath, &pebble.Options{
		Logger:   dal.NewPebbleLogger(s.logger),
		ReadOnly: true,
	})
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

	binary.BigEndian.PutUint64(buf, req.ChapterID)
	_, _ = hasher.Write(buf)

	binary.BigEndian.PutUint64(buf, req.CloseSequence)
	_, _ = hasher.Write(buf)

	if len(req.LastAuditHash) > 0 {
		_, _ = hasher.Write(req.LastAuditHash)
	}

	_, _ = hasher.Write(stateHash)

	sealingHash := hasher.Sum(nil)

	s.logger.WithFields(map[string]any{
		"chapterId":   req.ChapterID,
		"sealingHash": sealingHash,
	}).Infof("Chapter sealing complete, proposing SealChapter")

	// Propose the SealChapter order back into Raft
	if err := s.proposeFn(req.ChapterID, sealingHash, stateHash); err != nil {
		return fmt.Errorf("proposing SealChapter for chapter %d: %w", req.ChapterID, err)
	}

	return nil
}

// deterministicVTMarshaler matches the MarshalDeterministicVT method that
// vtprotobuf generates for messages annotated for deterministic marshaling.
// All three values hashed by computeStateHash (VolumePair, MetadataValue,
// TransactionState) implement it.
type deterministicVTMarshaler interface {
	MarshalDeterministicVT(dAtA []byte) []byte
}

func computeStateHash(reader dal.PebbleReader, attrs *attributes.Attributes) ([]byte, error) {
	hasher := blake3.New()

	// Reusable buffer for MarshalDeterministicVT — avoids one alloc per
	// entry across what can be a large iteration on a fully-sealed
	// checkpoint.
	var buf []byte

	if err := hashAttribute(reader, attrs.Volume, hasher, &buf, "volume"); err != nil {
		return nil, err
	}

	if err := hashAttribute(reader, attrs.Metadata, hasher, &buf, "metadata"); err != nil {
		return nil, err
	}

	if err := hashAttribute(reader, attrs.Transaction, hasher, &buf, "transaction state"); err != nil {
		return nil, err
	}

	return hasher.Sum(nil), nil
}

// hashAttribute streams every (key, value) pair in an attribute store into
// the hasher. Keys are taken in Pebble byte order (deterministic). Values
// are marshaled via MarshalDeterministicVT so the resulting hash is
// reproducible across nodes — important if/when state-hash comparison
// becomes a cross-node check (today only the leader computes the state
// hash and proposes it through Raft, so non-determinism would not
// surface, but the deterministic marshal closes a foot-gun on the
// sealing path).
//
// Cost: ~20% slower end-to-end than MarshalVT and ~75% slower per call
// (see #288). Acceptable here because sealing runs at most once per
// closing chapter, off the FSM hot path. The same swap on the hot path
// was rejected by the same analysis.
func hashAttribute[V proto.Message](
	reader dal.PebbleReader,
	attr *attributes.Attribute[V],
	hasher *blake3.Hasher,
	buf *[]byte,
	name string,
) error {
	iter, err := attr.NewStreamingIter(reader, nil)
	if err != nil {
		return fmt.Errorf("creating %s iterator: %w", name, err)
	}

	for iter.Next() {
		e := iter.Entry()

		_, _ = hasher.Write(e.CanonicalKey)

		m, ok := proto.Message(e.Value).(deterministicVTMarshaler)
		if !ok {
			_ = iter.Close()

			return fmt.Errorf("%s value type %T lacks MarshalDeterministicVT", name, e.Value)
		}

		*buf = m.MarshalDeterministicVT((*buf)[:0])
		_, _ = hasher.Write(*buf)
	}

	if closeErr := iter.Close(); closeErr != nil {
		return fmt.Errorf("closing %s iterator: %w", name, closeErr)
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("hashing %s entries: %w", name, err)
	}

	return nil
}
