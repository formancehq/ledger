package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Cross-store alignment for indexed queries (EN-1748). A filter tree mixes
// leaves from two stores — the main store (entity universe, tx-id ranges,
// reverted bitset, enrichment) and the read index (builtin/metadata indexes).
// The read index folds asynchronously behind the main store, so two
// independently-taken views disagree about the freshest commits: an entity
// visible in the main store whose index rows have not folded yet leaks
// through complements ("no timestamp") and mis-windows conjunctions. No
// single serialized state produces such a response.
//
// The alignment invariant: the read-index snapshot's fold cursor must cover
// the main reader's last applied sequence. The fold is ordered, so such a
// snapshot holds EVERY index row for the entities the main reader sees;
// membership can then only exceed the main store (entities committed after
// the handle), and MainHorizonKeep trims those back. The result is exactly
// the state at the main reader's sequence.
// alignWait bounds how long a query waits for the read index to fold up to
// the main handle's sequence. The fold normally trails by well under a
// millisecond; the bound bites only when the builder is stalled (backfill
// storm, faults), where failing fast with ErrReadIndexNotCaughtUp beats
// holding every request.
const alignWait = 2 * time.Second

// AlignedIndexSnapshot returns a read-index snapshot whose fold cursor is at
// or beyond mainReader's last applied log sequence, plus that sequence and a
// release closure. The cursor is verified through the snapshot itself, so the
// check can never race the snapshot it vouches for. Fails with
// ErrReadIndexNotCaughtUp when the fold does not catch up within alignWait.
//
// The release closure drops the read lease registered at the returned
// sequence — the pin the event GC must not reclaim past (read_lease.go). The
// caller must invoke it when iteration ends, alongside closing the snapshot.
//
// The pin is read from the handle before it can be registered, so a GC pass
// may begin reclaiming at or above it in that gap. Acquire refuses such a pin
// and the read fails with ErrReadPinReclaimed: mainReader is a fixed snapshot,
// so the pin cannot be moved forward here, and it has to keep matching the
// reader the caller enriches through. A retry opens a fresh handle, whose
// sequence is at or above the floor by construction.
func AlignedIndexSnapshot(ctx context.Context, rs *readstore.Store, mainReader dal.PebbleReader) (*pebble.Snapshot, uint64, func(), error) {
	mainSeq, err := ReadLastSequence(mainReader)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("reading main-store sequence: %w", err)
	}

	// A frozen store (query checkpoint) never advances, so waiting on it is
	// meaningless: the pair is consistent by construction at the checkpoint's
	// sequence (the builder materializes the read index as its fold crosses
	// the CreatedQueryCheckpoint log). Serve it as-is; the horizon filter
	// still bounds the index-ahead direction. No GC runs against a frozen
	// store, so there is no lease to hold.
	if rs.Frozen() {
		return rs.NewSnapshot(), mainSeq, func() {}, nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, alignWait)
	defer cancel()

	for {
		lease, ok := rs.Leases().Acquire(mainSeq)
		if !ok {
			return nil, 0, nil, &ErrReadPinReclaimed{Pin: mainSeq, Floor: rs.Leases().ReclaimFloor()}
		}

		snap := rs.NewSnapshot()

		lastIndexed, err := rs.LastIndexedSequenceFrom(snap)
		if err != nil {
			_ = snap.Close()
			lease.Release()

			return nil, 0, nil, fmt.Errorf("reading index progress: %w", err)
		}

		if lastIndexed >= mainSeq {
			return snap, mainSeq, lease.Release, nil
		}

		_ = snap.Close()
		lease.Release()

		if waitErr := rs.WaitForSequence(waitCtx, mainSeq); waitErr != nil {
			// A Pebble fault reading progress is not a freshness condition —
			// surfacing it as one would launder a real I/O error into a
			// retryable precondition.
			if !errors.Is(waitErr, context.DeadlineExceeded) && !errors.Is(waitErr, context.Canceled) {
				return nil, 0, nil, fmt.Errorf("waiting for read index: %w", waitErr)
			}

			return nil, 0, nil, &ErrReadIndexNotCaughtUp{Requested: mainSeq, Current: lastIndexed}
		}
	}
}

// MainHorizonKeep returns the FilterIterator predicate that trims an
// index-snapshot iteration back to the main reader's horizon: the aligned
// snapshot may have folded entities committed after the handle, which
// transaction enrichment through the main store cannot resolve. A
// transaction is admitted iff its row exists in the main reader (tx rows are
// never deleted within a live ledger, so absence means exactly "committed
// after the handle"); a log iff its id resolves through the index snapshot
// to a sequence at or below mainSeq.
//
// ACCOUNTS get no predicate: main-store existence is NOT a horizon signal
// there — a purged account legitimately lives on in the monotonic has-asset
// and metadata indexes while absent from the V+M universe, and account
// enrichment (scanAccount) renders absent accounts as address-only rows, so
// index-only members are servable as-is.
//
// Returns nil for targets with no trimmable cross-store membership (callers
// skip wrapping).
func MainHorizonKeep(
	target commonpb.QueryTarget,
	handle dal.PebbleReader,
	indexSnap dal.PebbleGetter,
	ledgerName string,
	mainSeq uint64,
) func([]byte) (bool, error) {
	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return func(e []byte) (bool, error) {
			if len(e) != 8 {
				return false, fmt.Errorf("horizon probe: transaction entity of unexpected length %d (want 8)", len(e))
			}

			return pebbleTxExists(handle, ledgerName, binary.BigEndian.Uint64(e))
		}
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		kb := dal.NewKeyBuilder()

		return func(e []byte) (bool, error) {
			if len(e) != 8 {
				return false, fmt.Errorf("horizon probe: log entity of unexpected length %d (want 8)", len(e))
			}

			v, closer, err := indexSnap.Get(readstore.LedgerLogKey(kb, ledgerName, binary.BigEndian.Uint64(e)))
			if err != nil {
				return false, fmt.Errorf("horizon probe: resolving log id: %w", err)
			}

			defer func() { _ = closer.Close() }()

			if len(v) != 8 {
				return false, fmt.Errorf("horizon probe: log index value of unexpected length %d (want 8)", len(v))
			}

			return binary.BigEndian.Uint64(v) <= mainSeq, nil
		}
	default:
		return nil
	}
}
