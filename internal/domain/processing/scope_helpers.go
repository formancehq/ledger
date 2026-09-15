package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// loadLedgerReader reads a ledger through the Scope and translates Scope-level
// errors into business errors handlers can return directly. ErrNotFound
// becomes ErrLedgerNotFound; an admission-contract violation (notably
// *state.ErrCoverageMiss) propagates verbatim so the audit chain records
// COVERAGE_MISS rather than a storage fault (EN-1379); any other error is
// wrapped in ErrStorageOperation so the FSM emits a failure audit entry.
//
// It returns the immutable reader view without cloning — the hot read-only
// path. Callers that only validate existence or compile account types use
// this instead of loadLedger so they never pay a LedgerInfo deep clone.
//
// A soft-deleted ledger resolves normally here, and three callers rely on
// that: DeleteLedger reads the tombstone it re-stamps, MirrorIngest is already
// rejected by its own loadBoundaries call (DeleteLedger drops the boundary row
// in the same apply), and the apply-children run behind processApply's gate.
// Handlers whose write must stay closed on a tombstone call
// loadLiveLedgerReader.
func loadLedgerReader(s Scope, name string) (commonpb.LedgerInfoReader, domain.Describable) {
	info, err := s.Ledgers().Get(domain.LedgerKey{Name: name})
	if errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrLedgerNotFound{Name: name}
	}

	if err != nil {
		return nil, domain.StoreFailure("loading ledger", err)
	}

	return info, nil
}

// loadLiveLedgerReader reads a ledger and rejects a soft-deleted one with
// ErrLedgerDeleted — the tombstone gate processApply applies before it
// dispatches any apply-scoped write. Handlers reached directly from
// processLedgerScoped never pass through processApply, so each one whose
// write must stay closed on a tombstone resolves its ledger here.
//
// The check reads DeletedAt off the reader loadLedgerReader already returned,
// so it consumes the SubAttrLedger coverage those orders declare and widens no
// read horizon (invariant #6). DeletedAt is replicated FSM state, so the
// rejection is identical on every replica (invariant #2).
func loadLiveLedgerReader(s Scope, name string) (commonpb.LedgerInfoReader, domain.Describable) {
	info, desc := loadLedgerReader(s, name)
	if desc != nil {
		return nil, desc
	}

	if info.GetDeletedAt() != nil {
		return nil, &domain.ErrLedgerDeleted{Name: name}
	}

	return info, nil
}

// loadLiveLedger is the loadLiveLedgerReader counterpart for the
// Mutate()-clone loader: same tombstone gate, applied before the clone so a
// rejected order pays no CloneVT.
func loadLiveLedger(s Scope, name string) (*commonpb.LedgerInfo, domain.Describable) {
	info, desc := loadLiveLedgerReader(s, name)
	if desc != nil {
		return nil, desc
	}

	return info.Mutate(), nil
}

// loadLedger reads a ledger and returns a Mutate()-clone so handlers can
// freely modify the result and write it back through s.PutLedger without
// mutating the cached pointer in place. Only configuration-mutating
// handlers should call this; read-only paths use loadLedgerReader.
func loadLedger(s Scope, name string) (*commonpb.LedgerInfo, domain.Describable) {
	info, desc := loadLedgerReader(s, name)
	if desc != nil {
		return nil, desc
	}

	return info.Mutate(), nil
}

// loadBoundaries mirrors loadLedger for the LedgerBoundaries channel.
func loadBoundaries(s Scope, name string) (raftcmdpb.LedgerBoundariesReader, domain.Describable) {
	boundaries, err := s.Boundaries().Get(domain.LedgerKey{Name: name})
	if errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrLedgerNotFound{Name: name}
	}

	if err != nil {
		return nil, domain.StoreFailure("loading boundaries", err)
	}

	return boundaries, nil
}
