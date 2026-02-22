package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateLedger(order *raftcmdpb.CreateLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	_, ok := s.GetLedger(order.Name)
	if ok {
		return nil, &ErrLedgerAlreadyExists{Name: order.Name}
	}

	ledgerID := s.IncrementNextLedgerID()
	info := &commonpb.LedgerInfo{
		Name:           order.Name,
		CreatedAt:      s.GetDate(),
		Id:             ledgerID,
		MetadataSchema: populateInitialSchema(order.InitialSchema),
	}
	s.PutLedger(order.Name, info)
	s.PutBoundaries(order.Name, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
		LedgerId:          ledgerID,
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
		return nil, &ErrLedgerNotFound{Name: order.Name}
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
