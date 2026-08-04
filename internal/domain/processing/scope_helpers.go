package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// loadLedger reads a ledger through the Scope and translates Scope-level
// errors into business errors handlers can return directly. ErrNotFound
// becomes ErrLedgerNotFound; an admission-contract violation (notably
// *state.ErrCoverageMiss) propagates verbatim so the audit chain records
// COVERAGE_MISS rather than a storage fault (EN-1379); any other error is
// wrapped in ErrStorageOperation so the FSM emits a failure audit entry.
//
// Returns a Mutate()-clone so handlers can freely modify the result and
// write it back through s.PutLedger without mutating the cached pointer
// in place. The clone cost is bounded (one CloneVT per handler invocation).
func loadLedger(s Scope, name string) (*commonpb.LedgerInfo, domain.Describable) {
	info, err := s.Ledgers().Get(domain.LedgerKey{Name: name})
	if errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrLedgerNotFound{Name: name}
	}

	if err != nil {
		return nil, domain.StoreFailure("loading ledger", err)
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
