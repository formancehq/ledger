package raftpb

import (
	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func (state *LedgerState) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		LedgerInfo        *commonpb.LedgerInfo `json:"ledgerInfo,omitempty"`
		NextLogId         uint64               `json:"nextLogId,omitempty"`
		NextTransactionId uint64               `json:"nextTransactionId,omitempty"`
	}{
		LedgerInfo:        state.LedgerInfo,
		NextLogId:         state.NextLogId,
		NextTransactionId: state.NextTransactionId,
	})
}

func (state *State) MarshalJSON() ([]byte, error) {
	ledgers := make(map[string]*LedgerState, len(state.Ledgers))
	for _, ledger := range state.Ledgers {
		ledgers[ledger.LedgerInfo.Name] = ledger
	}
	type Aux State
	return json.Marshal(struct {
		*Aux
		Ledgers map[string]*LedgerState `json:"ledgers"`
	}{
		Aux:     (*Aux)(state),
		Ledgers: ledgers,
	})
}

// MarshalJSON implements json.Marshaler for CreatedTransactionMemento
func (x *CreatedTransactionMemento) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction     *TransactionResume            `json:"transaction,omitempty"`
		AccountMetadata map[string]*commonpb.Metadata `json:"accountMetadata,omitempty"`
	}{
		Transaction:     x.Transaction,
		AccountMetadata: x.AccountMetadata,
	})
}

// MarshalJSON implements json.Marshaler for RevertedTransactionMemento
func (x *RevertedTransactionMemento) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		RevertedTransactionId uint64             `json:"revertedTransactionId,omitempty"`
		RevertTransaction     *TransactionResume `json:"revertTransaction,omitempty"`
	}{
		RevertedTransactionId: x.RevertedTransactionId,
		RevertTransaction:     x.RevertTransaction,
	})
}
