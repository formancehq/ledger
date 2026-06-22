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

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteLedger{
			DeleteLedger: &commonpb.DeletedLedgerLog{
				Name:      l.GetName(),
				DeletedAt: l.GetDeletedAt(),
			},
		},
	}, nil
}
