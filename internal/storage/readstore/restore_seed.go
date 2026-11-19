package readstore

import (
	"errors"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ValidateBackupSeed checks the immutable read-index checkpoint against its
// paired, later full primary checkpoint. Neither the live primary nor a primary
// with incremental exports applied is a valid substitute: a later history head
// can conceal a seed captured ahead of its full. A lagging projection is valid.
func ValidateBackupSeed(dir string, logger logging.Logger, primary dal.PebbleGetter, maxLogSequence, maxAuditSequence uint64) (retErr error) {
	db, err := openSeed(dir, logger, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()

	return validateSeed(db, primary, maxLogSequence, maxAuditSequence)
}

// PrepareRestoreSeed prepares a private, downloaded read-index checkpoint for
// use by a new cluster. Call before publishing the directory. Native cursors,
// event stamps and backfill state use portable history sequences. The two Raft
// certificates are only meaningful in the source cluster: both must disappear
// before either indexer can certify the destination's complete restored head.
func PrepareRestoreSeed(dir string, logger logging.Logger, primary dal.PebbleGetter, maxLogSequence, maxAuditSequence uint64) (retErr error) {
	db, err := openSeed(dir, logger, false)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()

	if err := validateSeed(db, primary, maxLogSequence, maxAuditSequence); err != nil {
		return err
	}

	batch := db.NewBatch()
	defer func() { retErr = errors.Join(retErr, batch.Close()) }()
	for _, key := range [][]byte{ReadRaftProgressKey(), AuditRaftProgressKey()} {
		if err := batch.Delete(key, nil); err != nil {
			return fmt.Errorf("deleting source projection certificate: %w", err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("clearing source projection certificates: %w", err)
	}
	// The read store disables WAL. Make the deletion durable before returning
	// the prepared directory to the restore publisher.
	if err := db.Flush(); err != nil {
		return fmt.Errorf("flushing restored projection certificates: %w", err)
	}

	return nil
}

func openSeed(dir string, logger logging.Logger, readOnly bool) (*pebble.DB, error) {
	// This short-lived open touches a handful of keys; bound table metadata as
	// for dal.OpenReadOnly so a multi-TB backup doesn't warm a second cache.
	db, err := pebble.Open(dir, &pebble.Options{
		Comparer:         ReadStoreComparer,
		Logger:           dal.NewPebbleLogger(logger),
		MaxOpenFiles:     32,
		ReadOnly:         readOnly,
		DisableWAL:       !readOnly,
		ErrorIfNotExists: true,
		// Staging writes exactly two tombstones. Do not compact the backup or
		// wait for a disabled compactor if its existing L0 exceeds the normal
		// live write-stall threshold when Flush rotates this one memtable.
		L0StopWritesThreshold:       math.MaxInt,
		DisableAutomaticCompactions: true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening read-index seed: %w", err)
	}

	return db, nil
}

func validateSeed(seed, primary dal.PebbleGetter, maxLogSequence, maxAuditSequence uint64) error {
	log, err := seedAnchor[*commonpb.Log](seed, primary, progressCursor, "log", dal.SubHistoryLog, maxLogSequence)
	if err != nil {
		return err
	}
	if _, err := seedAnchor[*auditpb.AuditEntry](seed, primary, auditCursor, "audit", dal.SubHistoryAudit, maxAuditSequence); err != nil {
		return err
	}
	// AppliedProposal shares audit's native sequence space but is sparse: a
	// failed proposal has an audit entry and no AppliedProposal. Check its row
	// and that resuming after it won't skip a not-yet-folded log of the batch.
	proposal, err := seedAnchor[*proposalpb.AppliedProposal](seed, primary, appliedProposalCursor, "applied proposal", dal.SubHistoryAppliedProposal, maxAuditSequence)
	if err != nil {
		return err
	}
	if proposal.GetMaxLogSequence() > log.GetSequence() {
		return fmt.Errorf("read-index seed: applied proposal %d ends at log %d beyond folded log cursor %d", proposal.GetSequence(), proposal.GetMaxLogSequence(), log.GetSequence())
	}

	return nil
}

// A missing native cursor means that worker starts at sequence zero. For a
// nonzero cursor, verify that the corresponding record exists in the full.
func seedAnchor[T interface {
	proto.Message
	GetSequence() uint64
}](seed, primary dal.PebbleGetter, cursor Uint64Cursor, name string, sub byte, head uint64) (T, error) {
	var zero T
	seq, err := cursor.Read(seed)
	if err != nil {
		return zero, fmt.Errorf("reading read-index seed %s cursor: %w", name, err)
	}
	if seq == 0 {
		return zero, nil
	}
	if seq > head {
		return zero, fmt.Errorf("read-index seed: %s cursor %d exceeds full checkpoint head %d", name, seq, head)
	}
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, sub).PutUint64(seq).Build()
	row, err := dal.ReadProto[T](primary, key)
	if err != nil {
		return zero, fmt.Errorf("reading full checkpoint %s at sequence %d: %w", name, seq, err)
	}
	// Generated GetSequence returns zero for an absent row.
	if row.GetSequence() != seq {
		return zero, fmt.Errorf("read-index seed: %s cursor %d has no matching record in full checkpoint", name, seq)
	}

	return row, nil
}
