package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateLedger(order *raftcmdpb.CreateLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, ok := s.GetLedger(order.GetName())
	if ok {
		if existing.GetDeletedAt() != nil {
			return nil, &domain.ErrLedgerDeleted{Name: order.GetName()}
		}

		return nil, &domain.ErrLedgerAlreadyExists{Name: order.GetName()}
	}

	// Validate initial account types if provided
	for name, at := range order.GetAccountTypes() {
		if err := accounttype.ValidatePattern(at.GetPattern()); err != nil {
			return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
		}
		at.Name = name
		at.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE
	}

	createdAt := s.GetDate()

	info := &commonpb.LedgerInfo{
		Name:                   order.GetName(),
		CreatedAt:              createdAt,
		MetadataSchema:         populateInitialSchema(order.GetInitialSchema()),
		Mode:                   order.GetMode(),
		MirrorSource:           order.GetMirrorSource(),
		AccountTypes:           order.GetAccountTypes(),
		DefaultEnforcementMode: order.GetDefaultEnforcementMode(),
	}
	s.PutLedger(order.GetName(), info)
	s.PutBoundaries(order.GetName(), &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	})

	// Build the log from the order — NOT from `info` which is the mutable
	// store object. MetadataSchema is built separately to avoid sharing
	// mutable maps between the store and the immutable log payload.
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Name:                   order.GetName(),
				CreatedAt:              createdAt,
				MetadataSchema:         populateInitialSchema(order.GetInitialSchema()),
				Mode:                   order.GetMode(),
				MirrorSource:           order.GetMirrorSource(),
				AccountTypes:           order.GetAccountTypes(),
				DefaultEnforcementMode: order.GetDefaultEnforcementMode(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteLedger(order *raftcmdpb.DeleteLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	l, ok := s.GetLedger(order.GetName())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetName()}
	}

	l.DeletedAt = s.GetDate()

	s.PutLedger(order.GetName(), l)
	s.MarkLedgerForCleanup(order.GetName())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteLedger{
			DeleteLedger: &commonpb.DeleteLedgerLog{
				Name:      l.GetName(),
				DeletedAt: l.GetDeletedAt(),
			},
		},
	}, nil
}
