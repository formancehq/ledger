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

type readBarrierHorizonKey struct{}

type queryHandleStore interface {
	NewReadHandle() (*dal.ReadHandle, error)
}

// WithReadBarrierHorizon binds the linearizable ReadIndex result to the local
// execution context. Stale reads deliberately omit it.
func WithReadBarrierHorizon(ctx context.Context, horizon uint64) context.Context {
	return context.WithValue(ctx, readBarrierHorizonKey{}, horizon)
}

// ReadBarrierHorizon returns the ReadIndex horizon carried by a local routed
// read, when the request asked for linearizable consistency.
func ReadBarrierHorizon(ctx context.Context) (uint64, bool) {
	h, ok := ctx.Value(readBarrierHorizonKey{}).(uint64)

	return h, ok
}

func mainAppliedHorizon(ctx context.Context, mainReader dal.PebbleGetter) (uint64, error) {
	horizon, err := ReadLastAppliedIndex(mainReader)
	if err != nil {
		return 0, fmt.Errorf("reading main-store applied index: %w", err)
	}
	if barrier, ok := ReadBarrierHorizon(ctx); ok && horizon < barrier {
		return 0, fmt.Errorf(
			"main-store snapshot applied index %d is behind ReadIndex horizon %d",
			horizon, barrier,
		)
	}

	return horizon, nil
}

// AlignmentOwed reports whether a query of this shape must wait for the read
// index to certify the main reader's applied horizon before it can be answered.
//
// Only a read that consults the index is owed alignment. An unfiltered
// ACCOUNTS or TRANSACTIONS query draws its universe from the main store
// (compileUniverse iterates the main reader) and enriches from that same
// reader, so it never observes the two stores at different fold points —
// waiting for the fold would gate it on data it does not read, and a lagging
// or stopped builder would stall a read that is independent of it. The same
// holds for filters whose complete tree is served by the main store, such as
// transaction ID/reverted predicates and account-address predicates. LOGS is
// not in that class even unfiltered: its universe is the read index's
// per-ledger log limb.
func AlignmentOwed(filter *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
		return true
	}

	return filterUsesReadIndex(filter, target)
}

// filterUsesReadIndex mirrors the storage choice made by compile. Boolean
// nodes use the read index as soon as any descendant does; NOT also compiles a
// universe, but ACCOUNTS and TRANSACTIONS universes come from the main store.
// Unknown/malformed leaves are left to Compile's fail-loud validation and do
// not acquire an otherwise unused projection wait first.
func filterUsesReadIndex(filter *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	if filter == nil {
		return false
	}

	switch f := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range f.And.GetFilters() {
			if filterUsesReadIndex(child, target) {
				return true
			}
		}

		return false
	case *commonpb.QueryFilter_Or:
		for _, child := range f.Or.GetFilters() {
			if filterUsesReadIndex(child, target) {
				return true
			}
		}

		return false
	case *commonpb.QueryFilter_Not:
		return filterUsesReadIndex(f.Not.GetFilter(), target)
	case *commonpb.QueryFilter_Address:
		return target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	case *commonpb.QueryFilter_BuiltinUint:
		return f.BuiltinUint.GetField() != commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID
	case *commonpb.QueryFilter_Reverted:
		return false
	case *commonpb.QueryFilter_Field,
		*commonpb.QueryFilter_Reference,
		*commonpb.QueryFilter_LogId,
		*commonpb.QueryFilter_LogBuiltinUint,
		*commonpb.QueryFilter_AccountHasAsset:
		return true
	case *commonpb.QueryFilter_Ledger:
		return target == commonpb.QueryTarget_QUERY_TARGET_LOGS
	default:
		return false
	}
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
// The alignment invariant uses two deliberately distinct positions. The main
// reader supplies a fixed durable Raft applied index H; the read-index snapshot
// must carry a Raft certificate >= H. The projection publishes that certificate
// only after folding every native log visible in its source snapshot bounded by
// H, and atomically with the target-completing batch. Its native log cursor is
// retained separately for fold resumption, event resolution and trimming.
//
// AlignedIndexSnapshot returns the certified projection snapshot, the main
// reader's native log sequence, and a release closure. The certificate is read
// back through the snapshot itself, so the check cannot race the view it
// vouches for. A linearizable routed read also carries its ReadIndex result R;
// the already-open main snapshot must prove H >= R. Stale reads omit R but use
// the same fixed local H and projection alignment.
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
// not optional — a filtered read cannot answer correctly until the projection
// certifies H — so how much latency to spend belongs to the caller. H is fixed
// for the life of the handle: the wait never chases the moving store head.
//
// The lease is released before waiting, so a long wait pins no history and
// creates no reclamation pressure.
func AlignedIndexSnapshot(ctx context.Context, rs *readstore.Store, mainReader dal.PebbleReader, releaseHold func()) (*pebble.Snapshot, uint64, func(), error) {
	mainSeq, err := ReadLastSequence(mainReader)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("reading main-store sequence: %w", err)
	}
	mainAppliedIndex, err := mainAppliedHorizon(ctx, mainReader)
	if err != nil {
		return nil, 0, nil, err
	}

	// A frozen store (query checkpoint) never advances. The builder publishes
	// its .ready marker only after every promised projection has certified the
	// checkpoint's applied index; still re-read that certificate below so a
	// malformed or incomplete checkpoint fails rather than waiting forever. No
	// GC runs against a frozen store, so there is no lease to hold.
	for {
		var releaseLease func()
		if rs.Frozen() {
			releaseLease = func() {}
		} else if lease, ok := rs.Leases().Pin(mainSeq); !ok {
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
		} else {
			releaseLease = lease.Release
		}

		snap := rs.NewSnapshot()

		lastIndexed, err := rs.ReadRaftProgressFrom(snap)
		if err != nil {
			_ = snap.Close()
			releaseLease()

			return nil, 0, nil, fmt.Errorf("reading index progress: %w", err)
		}

		if lastIndexed >= mainAppliedIndex {
			releaseHold()

			return snap, mainSeq, releaseLease, nil
		}

		_ = snap.Close()
		releaseLease()

		if rs.Frozen() {
			return nil, 0, nil, fmt.Errorf(
				"frozen read projection at Raft index %d is behind main checkpoint horizon %d",
				lastIndexed, mainAppliedIndex,
			)
		}

		if waitErr := rs.WaitForRaftProgress(ctx, mainAppliedIndex); waitErr != nil {
			// The caller's context ending is the caller's answer; a Pebble
			// fault reading progress is a real I/O error and must not be
			// laundered into a freshness condition.
			return nil, 0, nil, fmt.Errorf("waiting for read projection alignment at Raft index %d: %w", mainAppliedIndex, waitErr)
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
func OpenQueryHandle(rs *readstore.Store, store queryHandleStore, filter *commonpb.QueryFilter, target commonpb.QueryTarget) (*dal.ReadHandle, func(), error) {
	return openQueryHandle(rs, store, AlignmentOwed(filter, target))
}

// OpenReservedQueryHandle opens the main snapshot before a consumer whose
// need for native event history cannot be described by an API query filter.
// The short-lived reservation closes the reclamation race until the consumer
// either installs a lease through AlignedIndexSnapshot or proves that it does
// not need history and releases the reservation.
func OpenReservedQueryHandle(rs *readstore.Store, store queryHandleStore) (*dal.ReadHandle, func(), error) {
	return openQueryHandle(rs, store, true)
}

func openQueryHandle(rs *readstore.Store, store queryHandleStore, reserve bool) (*dal.ReadHandle, func(), error) {
	if rs.Frozen() || !reserve {
		// A checkpoint's index never advances and nothing reclaims against
		// it, so there is no floor to hold either.
		handle, err := store.NewReadHandle()

		return handle, func() {}, err
	}

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
