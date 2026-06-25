package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// loadLedger reads a ledger through the Scope and translates Scope-level
// errors into business errors handlers can return directly. ErrNotFound
// becomes ErrLedgerNotFound; any other error (notably *ErrCoverageMiss)
// is wrapped in ErrStorageOperation so the FSM emits a failure audit
// entry rather than misreporting the cause.
//
// Returns a Mutate()-clone so handlers can freely modify the result and
// write it back through s.PutLedger without mutating the cached pointer
// in place. The clone cost is bounded (one CloneVT per handler invocation).
func loadLedger(s Scope, name string) (*commonpb.LedgerInfo, domain.Describable) {
	info, err := s.GetLedger(name)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrLedgerNotFound{Name: name}
	}

	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "loading ledger", Cause: err}
	}

	return info.Mutate(), nil
}

// loadBoundaries mirrors loadLedger for the LedgerBoundaries channel.
func loadBoundaries(s Scope, name string) (raftcmdpb.LedgerBoundariesReader, domain.Describable) {
	boundaries, err := s.GetBoundaries(name)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrLedgerNotFound{Name: name}
	}

	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "loading boundaries", Cause: err}
	}

	return boundaries, nil
}
