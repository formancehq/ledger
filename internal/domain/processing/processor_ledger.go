package processing

import (
	"errors"
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processCreateLedger(ledger string, order *raftcmdpb.CreateLedgerOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
	existing, err := s.Ledgers().Get(domain.LedgerKey{Name: ledger})
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrStorageOperation{Operation: "loading ledger", Cause: err}
	}

	if err == nil {
		if existing.GetDeletedAt() != nil {
			return nil, &domain.ErrLedgerDeleted{Name: ledger}
		}

		return nil, &domain.ErrLedgerAlreadyExists{Name: ledger}
	}

	// Validate initial account types if provided, then build a canonical map
	// for derived state. Iterate names in sorted order so the first invalid
	// pattern reported (chain-bound ErrInvalidPattern → AuditFailure) is
	// identical on every replica — a raw map range could surface a different
	// offending pattern per node (invariant #2). See EN-1521.
	//
	// The canonical map holds CLONES of the order-owned messages with each
	// clone's Name set to the authoritative map key. We MUST NOT mutate the
	// messages reachable from order.AccountTypes: the accepted order is
	// captured verbatim by marshalOrdersForAudit at apply time, so mutating
	// an embedded Name here would make the audited order diverge from what
	// was accepted — and, with an idempotency key, make the pre-processing
	// frozen outcome hash disagree with the checker's hash re-derived from
	// the audited order (EN-1533). A nil map value has GetPattern()=="" and
	// is rejected by ValidatePattern ("pattern must not be empty") before the
	// clone, so every entry cloned below is guaranteed non-nil.
	var canonicalAccountTypes map[string]*commonpb.AccountType
	if src := order.GetAccountTypes(); len(src) > 0 {
		canonicalAccountTypes = make(map[string]*commonpb.AccountType, len(src))
	}
	for _, name := range slices.Sorted(maps.Keys(order.GetAccountTypes())) {
		at := order.GetAccountTypes()[name]
		if err := accounttype.ValidatePattern(at.GetPattern()); err != nil {
			return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
		}
		clone := at.CloneVT()
		clone.Name = name
		canonicalAccountTypes[name] = clone
	}

	createdAt := s.GetDate().Mutate()
	ledgerID := s.IncrementNextLedgerID()

	info := &commonpb.LedgerInfo{
		Name:                   ledger,
		Id:                     ledgerID,
		CreatedAt:              createdAt,
		MetadataSchema:         populateInitialSchema(order.GetInitialSchema()),
		Mode:                   order.GetMode(),
		MirrorSource:           order.GetMirrorSource(),
		AccountTypes:           canonicalAccountTypes,
		DefaultEnforcementMode: order.GetDefaultEnforcementMode(),
	}
	s.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)
	s.Boundaries().Put(domain.LedgerKey{Name: ledger}, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	})

	// The MirrorConfigChange signal (post-commit mirror worker
	// reconciliation) is derived from CreatedLedgerLog.Mode == MIRROR by
	// deriveSignals — see processor.go.

	// Build the log its OWN immutable snapshot — NOT `info.AccountTypes`
	// (mutated in place by processAddAccountType) nor the order-owned messages
	// (captured verbatim for audit). Clone each canonical entry so the log map
	// shares no mutable map or message with either the store or the order. The
	// clones carry the same map-key canonical Name, so CreatedLedgerLog.
	// ToLedgerInfo() reconstructs a LedgerInfo identical to the FSM-built one.
	var logAccountTypes map[string]*commonpb.AccountType
	if len(canonicalAccountTypes) > 0 {
		logAccountTypes = make(map[string]*commonpb.AccountType, len(canonicalAccountTypes))
		for k, v := range canonicalAccountTypes {
			logAccountTypes[k] = v.CloneVT()
		}
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name:                   ledger,
				Id:                     ledgerID,
				CreatedAt:              createdAt,
				MetadataSchema:         populateInitialSchema(order.GetInitialSchema()),
				Mode:                   order.GetMode(),
				MirrorSource:           order.GetMirrorSource(),
				AccountTypes:           logAccountTypes,
				DefaultEnforcementMode: order.GetDefaultEnforcementMode(),
			},
		},
	}, nil
}

func processDeleteLedger(ledger string, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
	l, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	l.DeletedAt = s.GetDate().Mutate()

	s.Ledgers().Put(domain.LedgerKey{Name: ledger}, l)
	// The LedgerCleanup signal (cleanup queue + boundary overlay drop) is
	// derived from DeletedLedgerLog by deriveSignals.

	// Index registry entries scoped to this ledger are NOT cleared here:
	//   - processApply rejects every same-batch Apply order with
	//     ErrLedgerDeleted (info.DeletedAt != nil), so no later order in
	//     this proposal will read a stale Index row.
	//   - MarkLedgerForCleanup queues a Pebble range delete on
	//     [SubAttrIndex][ledgerName padded] (see batch.deleteLedgerData)
	//     which purges every entry — cache-resident or not.
	//   - All read paths (query.Compile, BucketService.ListIndexes) gate on
	//     LedgerInfo.DeletedAt and surface NotFound before reaching the
	//     registry, so orphan cache entries are unreachable.
	// An in-batch cache-iteration drop would only matter for a pathological
	// "delete then create on the same ledger" batch the DeletedAt guard
	// already rejects, and it would bypass the coverage gate (KeyStore.M
	// iter has no preload declaration). Dropping the loop keeps the
	// coverage invariant intact.

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteLedger{
			DeleteLedger: &commonpb.DeletedLedgerLog{
				Name:      l.GetName(),
				DeletedAt: l.GetDeletedAt(),
			},
		},
	}, nil
}
