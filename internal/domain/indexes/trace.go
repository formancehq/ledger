package indexes

import (
	"sync"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Diagnostic scaffolding for the index-registry loss hunt: a per-process record
// of the last mutation this replica applied to each registry key. Put and
// Remove are the registry's only writers and both run on the FSM apply path, so
// a lookup that misses a key whose last recorded op was a write means the entry
// disappeared without anything deleting it.
//
// Process-local and never read by the state machine — it records what a replica
// did, so it cannot influence what a replica decides.
var opTrace sync.Map // traceKey -> bool (true: last op was a write)

type traceKey struct {
	ledger    string
	canonical string
}

func recordOp(ledgerName string, id *commonpb.IndexID, wrote bool) {
	if id == nil {
		return
	}

	opTrace.Store(traceKey{ledger: ledgerName, canonical: Canonical(id)}, wrote)
}

// MarkPresent records an entry this replica found in the registry at boot. A
// process-local trace of writes alone goes blind across a restart — the replica
// forgets every entry written before it booted, which is exactly when a loss
// following a node kill would go unseen. Seeding from what storage holds also
// sharpens the claim: a later miss then means storage has the entry and the
// apply path could not see it.
func MarkPresent(ledgerName string, id *commonpb.IndexID) {
	recordOp(ledgerName, id, true)
}

// LastOpWasWrite reports whether the last registry mutation this replica applied
// for (ledgerName, id) was a write, or the entry was present at boot — either
// way the entry should still be there. False when the key was never seen here or
// was deleted.
func LastOpWasWrite(ledgerName string, id *commonpb.IndexID) bool {
	if id == nil {
		return false
	}

	wrote, ok := opTrace.Load(traceKey{ledger: ledgerName, canonical: Canonical(id)})

	return ok && wrote.(bool)
}
