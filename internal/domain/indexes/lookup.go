package indexes

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source=lookup.go -destination=lookup_generated_test.go -package=indexes_test Lookup,IndexWriter

// Lookup is implemented by anything that can serve a point lookup on the
// bucket-scoped index registry. The FSM hot path passes Scope.Indexes()
// (a processing.Accessor whose Get satisfies this shape); read-side
// handlers pass a Pebble-backed view through the readstore. The returned
// value is a commonpb.IndexReader so callers cannot mutate the
// cache-resident proto in place — mirror the discipline that
// raftcmdpb.LedgerBoundariesReader / VolumePairReader enforce for the
// other hot-path attribute kinds (#496).
//
// Return contract:
//   - (idx, nil): entry exists and was returned.
//   - (nil, domain.ErrNotFound): entry legitimately absent.
//   - (nil, other err): infrastructure failure (Pebble I/O), or, on the
//     FSM hot path, an *ErrCoverageMiss when the proposer did not declare
//     the key — the apply path bubbles it up as a business rejection.
type Lookup interface {
	Get(key domain.IndexKey) (commonpb.IndexReader, error)
}

// IndexWriter is implemented by the FSM-apply Accessor returned from
// Scope.Indexes(). Method names match Accessor so the same instance
// satisfies both Lookup and IndexWriter without an adapter. Delete
// returns an error so a coverage miss (invariant #6) propagates rather
// than silently dropping.
type IndexWriter interface {
	Put(key domain.IndexKey, idx *commonpb.Index)
	Delete(key domain.IndexKey) error
}

// KeyFor builds the registry key for an index. An empty ledgerName addresses
// the bucket-scoped slot (e.g. audit indexes); a non-empty name is the
// ledger-scoped slot.
func KeyFor(ledgerName string, id *commonpb.IndexID) domain.IndexKey {
	return domain.IndexKey{LedgerName: ledgerName, Canonical: Canonical(id)}
}

// Find returns the Index entry for the (ledgerName, id) tuple.
//
//   - (idx, nil) on hit. The returned value is a Reader; callers that need
//     to mutate (e.g. flip BuildStatus) MUST call Mutate() to obtain a
//     writable clone before Put-ing it back through the IndexWriter.
//   - (nil, nil) on legitimate absence (domain.ErrNotFound from the lookup).
//   - (nil, err) for any other error (notably *state.ErrCoverageMiss on the
//     FSM hot path); callers MUST propagate so the apply rejects the order.
//
// A nil lookup is treated as "no indexes registered" — returns (nil, nil) so
// query.Compile / tests can drive the compiler without wiring a registry.
func Find(r Lookup, ledgerName string, id *commonpb.IndexID) (commonpb.IndexReader, error) {
	if r == nil || id == nil {
		return nil, nil
	}

	idx, err := r.Get(KeyFor(ledgerName, id))
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return idx, nil
}

// IsReady reports whether the index at (ledgerName, id) is registered and READY.
// Infrastructure errors collapse to "not ready"; callers that need to surface
// them must call Find directly.
func IsReady(r Lookup, ledgerName string, id *commonpb.IndexID) bool {
	idx, err := Find(r, ledgerName, id)
	if err != nil || idx == nil {
		return false
	}

	return idx.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
}

// Status returns the build status for (ledgerName, id), or UNSPECIFIED if absent
// (or on any read error). Callers that need to distinguish "absent" from
// "UNSPECIFIED but registered" should call Find directly.
func Status(r Lookup, ledgerName string, id *commonpb.IndexID) commonpb.IndexBuildStatus {
	idx, err := Find(r, ledgerName, id)
	if err != nil || idx == nil {
		return commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_UNSPECIFIED
	}

	return idx.GetBuildStatus()
}

// Put upserts an Index entry. The caller is responsible for ensuring
// idx.GetLedger() matches ledgerName (empty for bucket-scope).
func Put(w IndexWriter, ledgerName string, idx *commonpb.Index) {
	if idx == nil || idx.GetId() == nil {
		return
	}

	w.Put(KeyFor(ledgerName, idx.GetId()), idx)
}

// Remove deletes the Index entry matching (ledgerName, id). Silently no-ops on
// nil ids so callers can pipe order payloads without explicit validation.
// Returns any error the underlying writer surfaces (e.g. *state.ErrCoverageMiss
// on the FSM hot path when the proposer did not declare the deletion key).
func Remove(w IndexWriter, ledgerName string, id *commonpb.IndexID) error {
	if id == nil {
		return nil
	}

	return w.Delete(KeyFor(ledgerName, id))
}
