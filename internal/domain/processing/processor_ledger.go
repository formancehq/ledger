package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateLedger(order *raftcmdpb.CreateLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	_, ok := s.GetLedger(order.GetName())
	if ok {
		return nil, &domain.ErrLedgerAlreadyExists{Name: order.GetName()}
	}

	// Validate chart at creation time if provided
	if order.GetChartOfAccounts() != nil {
		err := validateChart(order.GetChartOfAccounts())
		if err != nil {
			return nil, &domain.ErrInvalidChart{Details: err.Error()}
		}
	}

	info := &commonpb.LedgerInfo{
		Name:            order.GetName(),
		CreatedAt:       s.GetDate(),
		MetadataSchema:  populateInitialSchema(order.GetInitialSchema()),
		Mode:            order.GetMode(),
		MirrorSource:    order.GetMirrorSource(),
		ChartOfAccounts: order.GetChartOfAccounts(),
		EnforcementMode: order.GetEnforcementMode(),
	}
	s.PutLedger(order.GetName(), info)
	s.PutBoundaries(order.GetName(), &raftcmdpb.LedgerBoundaries{
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
	l, ok := s.GetLedger(order.GetName())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetName()}
	}

	l.DeletedAt = s.GetDate()

	s.PutLedger(order.GetName(), l)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteLedger{
			DeleteLedger: &commonpb.DeleteLedgerLog{
				Info: l,
			},
		},
	}, nil
}
