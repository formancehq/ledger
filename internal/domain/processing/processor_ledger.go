package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateLedger(ledger string, order *raftcmdpb.CreateLedgerOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	existing, err := s.GetLedger(ledger)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrStorageOperation{Operation: "loading ledger", Cause: err}
	}

	if err == nil {
		if existing.GetDeletedAt() != nil {
			return nil, &domain.ErrLedgerDeleted{Name: ledger}
		}

		return nil, &domain.ErrLedgerAlreadyExists{Name: ledger}
	}

	// Validate initial account types if provided
	for name, at := range order.GetAccountTypes() {
		if err := accounttype.ValidatePattern(at.GetPattern()); err != nil {
			return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
		}
		at.Name = name
	}

	createdAt := s.GetDate()
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
	s.PutLedger(ledger, info)
	s.PutBoundaries(ledger, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	})

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

func (p *RequestProcessor) processDeleteLedger(ledger string, s Scope) (*commonpb.LogPayload, domain.Describable) {
	l, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	l = l.CloneVT()

	l.DeletedAt = s.GetDate()

	s.PutLedger(ledger, l)
	s.MarkLedgerForCleanup(ledger)

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
