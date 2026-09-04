package query

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// AlignmentOwed reports whether a query of this shape must wait for the read
// index to fold up to the main reader's sequence before it can be answered.
//
// Only a read that consults the index is owed alignment. An unfiltered
// ACCOUNTS or TRANSACTIONS query draws its universe from the main store
// (compileUniverse iterates the main reader) and enriches from that same
// reader, so it never observes the two stores at different fold points —
// waiting for the fold would gate it on data it does not read, and a lagging
// or stopped builder would stall a read that is independent of it. LOGS is
// not in that class even unfiltered: its universe is the read index's
// per-ledger log limb.
func AlignmentOwed(filter *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	return filter != nil || target == commonpb.QueryTarget_QUERY_TARGET_LOGS
}

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
// AlignedIndexSnapshot returns a read-index snapshot whose fold cursor is at
// or beyond mainReader's last applied log sequence, plus that sequence and a
// release closure. The cursor is verified through the snapshot itself, so the
// check can never race the snapshot it vouches for. Waits for the fold for as
// long as the caller's context allows.
//
// The release closure drops the read lease registered at the returned
// sequence — the pin the event GC must not reclaim past (read_lease.go). The
// caller must invoke it when iteration ends, alongside closing the snapshot.
//
// Callers must obtain mainReader from OpenQueryHandle, passing the same
// filter and target, and hand its release closure here as releaseHold: it
// reserves the reclaim floor before opening the handle, so the pin registered
// here can never be refused. The reservation is dropped as soon as that pin
// exists — from then on the read's own lease retains everything it needs,
// while the reservation would only drag the GC watermark back down to the
// fold cursor as of the request's start, for every request in flight.
//
// The wait is bounded by the caller's context and nothing else. Alignment is
// not optional — a filtered read cannot answer correctly until the fold
// reaches the pin — so how long to spend on it is the caller's call, exactly
// as a deadline expresses. A server-side cap would also diverge rather than
// converge: mainSeq is fixed for the life of this handle, so waiting makes
// progress, while a retry opens a NEW handle at a HIGHER sequence and leaves
// the fold further behind than the attempt that just gave up. Under sustained
// write load that never converges.
//
// The lease is released before waiting, so a long wait pins no history and
// creates no reclamation pressure.
func AlignedIndexSnapshot(ctx context.Context, rs *readstore.Store, mainReader dal.PebbleReader, releaseHold func()) (*pebble.Snapshot, uint64, func(), error) {
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
		releaseHold()

		return rs.NewSnapshot(), mainSeq, func() {}, nil
	}

	for {
		lease, ok := rs.Leases().Pin(mainSeq)
		if !ok {
			// OpenQueryHandle holds the floor across the handle's creation,
			// so a pin beneath it means this read bypassed that helper and
			// may resolve a group whose history is already reclaimed. There is
			// no recovery here: the pin cannot move (the handle is a fixed
			// snapshot) and it must keep matching the reader enrichment uses.
			assert.Unreachable("query: aligned read pinned below the reclaim floor", map[string]any{
				"pin":   mainSeq,
				"floor": rs.Leases().ReclaimFloor(),
			})

			return nil, 0, nil, fmt.Errorf(
				"invariant: aligned read pinned at %d, below the reclaim floor %d — the handle was not opened through OpenQueryHandle",
				mainSeq, rs.Leases().ReclaimFloor(),
			)
		}

		snap := rs.NewSnapshot()

		lastIndexed, err := rs.LastIndexedSequenceFrom(snap)
		if err != nil {
			_ = snap.Close()
			lease.Release()

			return nil, 0, nil, fmt.Errorf("reading index progress: %w", err)
		}

		if lastIndexed >= mainSeq {
			releaseHold()

			return snap, mainSeq, lease.Release, nil
		}

		_ = snap.Close()
		lease.Release()

		if waitErr := rs.WaitForSequence(ctx, mainSeq); waitErr != nil {
			// The caller's context ending is the caller's answer; a Pebble
			// fault reading progress is a real I/O error and must not be
			// laundered into a freshness condition.
			return nil, 0, nil, fmt.Errorf("waiting for read index alignment at sequence %d: %w", mainSeq, waitErr)
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
// enrichment (scanAccount) renders absent accounts as address-only rows.
// The account leaves trim themselves instead: metadata membership resolves
// through event keys at the pin, and the has-asset scan is gated by each
// row's first-touch stamp (compileAccountHasAssetCondition) — so a purged
// account keeps serving while one whose first index row folded past the
// handle stays invisible, exactly the split main-store existence cannot
// express.
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

// OpenQueryHandle opens the main-store handle a query reads through, holding
// reclamation still across the handle's creation when — and only when — the
// query will align an index snapshot against it.
//
// For an aligned read the order is the point. A read's pin does not exist
// until its handle is open, so no lease can protect it beforehand; under
// sustained load the fold cursor — and with it the GC's reclaim floor —
// advances past a just-opened handle within a tick, and the read is refused
// for history it never had a chance to claim. Reserving first pins the floor
// where it is, and the handle that follows is necessarily at or above it.
//
// The returned release drops the reservation; callers must invoke it when the
// read finishes, alongside closing the handle. Holding it for the request's
// life is intended — a request retains exactly the history it may still need.
// That is also why an unaligned read must not take one: the reservation exists
// to make the later Acquire un-refusable, which a read that never resolves an
// event never performs, so it would hold the event GC's watermark for the life
// of a request — or of a streaming cursor — in exchange for nothing.
func OpenQueryHandle(rs *readstore.Store, store *dal.Store, filter *commonpb.QueryFilter, target commonpb.QueryTarget) (*dal.ReadHandle, func(), error) {
	if rs.Frozen() || !AlignmentOwed(filter, target) {
		// A checkpoint's index never advances and nothing reclaims against
		// it, so there is no floor to hold either.
		handle, err := store.NewReadHandle()

		return handle, func() {}, err
	}

	return openReservedHandle(rs, store)
}

// OpenIndexHandle is OpenQueryHandle for reads that are index reads by
// construction — InspectIndex scans an index keyspace directly — so alignment
// is always owed and the floor is always reserved (checkpoint stores
// excepted, as above).
func OpenIndexHandle(rs *readstore.Store, store *dal.Store) (*dal.ReadHandle, func(), error) {
	if rs.Frozen() {
		handle, err := store.NewReadHandle()

		return handle, func() {}, err
	}

	return openReservedHandle(rs, store)
}

func openReservedHandle(rs *readstore.Store, store *dal.Store) (*dal.ReadHandle, func(), error) {
	cursor, err := rs.LastIndexedSequence()
	if err != nil {
		return nil, nil, fmt.Errorf("reading index progress: %w", err)
	}

	hold := rs.Leases().Reserve(cursor)

	handle, err := store.NewReadHandle()
	if err != nil {
		hold.Release()

		return nil, nil, err
	}

	return handle, hold.Release, nil
}
