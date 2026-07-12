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

	// Validate initial account types if provided. Iterate names in sorted order
	// so the first invalid pattern reported (chain-bound ErrInvalidPattern →
	// AuditFailure) is identical on every replica — a raw map range could
	// surface a different offending pattern per node (invariant #2). See
	// EN-1521.
	for _, name := range slices.Sorted(maps.Keys(order.GetAccountTypes())) {
		at := order.GetAccountTypes()[name]
		if err := accounttype.ValidatePattern(at.GetPattern()); err != nil {
			return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
		}
		at.Name = name
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
		AccountTypes:           order.GetAccountTypes(),
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

	// Build the log from the order — NOT from `info` which is the mutable
	// store object. MetadataSchema and AccountTypes are cloned to avoid
	// sharing mutable maps/pointers between the store and the immutable
	// log payload.
	var logAccountTypes map[string]*commonpb.AccountType
	if src := order.GetAccountTypes(); len(src) > 0 {
		logAccountTypes = make(map[string]*commonpb.AccountType, len(src))
		for k, v := range src {
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
