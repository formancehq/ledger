package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateLedger(order *raftcmdpb.CreateLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	_, ok := s.GetLedger(order.Name)
	if ok {
		return nil, &domain.ErrLedgerAlreadyExists{Name: order.Name}
	}

	// Validate chart at creation time if provided
	if order.ChartOfAccounts != nil {
		if err := validateChart(order.ChartOfAccounts); err != nil {
			return nil, &domain.ErrInvalidChart{Details: err.Error()}
		}
	}

	info := &commonpb.LedgerInfo{
		Name:            order.Name,
		CreatedAt:       s.GetDate(),
		MetadataSchema:  populateInitialSchema(order.InitialSchema),
		Mode:            order.Mode,
		MirrorSource:    order.MirrorSource,
		ChartOfAccounts: order.ChartOfAccounts,
		EnforcementMode: order.EnforcementMode,
	}
	s.PutLedger(order.Name, info)
	s.PutBoundaries(order.Name, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	})

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreateLedgerLog{
				Info: info,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteLedger(order *raftcmdpb.DeleteLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	l, ok := s.GetLedger(order.Name)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.Name}
	}
	l.DeletedAt = s.GetDate()

	s.PutLedger(order.Name, l)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteLedger{
			DeleteLedger: &commonpb.DeleteLedgerLog{
				Info: l,
			},
		},
	}, nil
}
