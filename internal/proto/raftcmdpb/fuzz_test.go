package raftcmdpb

import (
	"testing"
)

// FuzzProposalUnmarshalVT fuzzes the binary protobuf decoder for Raft Proposals.
// A Proposal wraps one or more Orders and is the main entry point for all
// state machine mutations in the Raft consensus log.
func FuzzProposalUnmarshalVT(f *testing.F) {
	empty := &Proposal{Id: 1}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	withOrder := &Proposal{
		Id: 42,
		Orders: []*Order{
			{
				Type: &Order_CreateLedger{
					CreateLedger: &CreateLedgerOrder{
						Name: "default",
					},
				},
			},
		},
	}
	if data, err := withOrder.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x0A, 0x80, 0x80, 0x80, 0x80, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg Proposal
		_ = msg.UnmarshalVT(data)
	})
}

// FuzzOrderUnmarshalVT fuzzes the binary protobuf decoder for Raft Orders.
// Orders use a large oneof with 30+ variants (create ledger, apply, delete, etc.).
// This targets the dispatch logic and nested message parsing.
func FuzzOrderUnmarshalVT(f *testing.F) {
	orders := []Order{
		{Type: &Order_CreateLedger{CreateLedger: &CreateLedgerOrder{Name: "test"}}},
		{Type: &Order_DeleteLedger{DeleteLedger: &DeleteLedgerOrder{Name: "test"}}},
		{Type: &Order_Apply{Apply: &LedgerApplyOrder{Ledger: "test"}}},
	}
	for i := range orders {
		if data, err := orders[i].MarshalVT(); err == nil {
			f.Add(data)
		}
	}

	f.Add([]byte{})
	f.Add([]byte{0xFF})
	f.Add([]byte{0xF8, 0x01, 0x01}) // unknown field number

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg Order
		_ = msg.UnmarshalVT(data)
	})
}
