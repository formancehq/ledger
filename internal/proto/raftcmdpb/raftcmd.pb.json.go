package raftcmdpb

import (
	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// MarshalJSON implements json.Marshaler for CreatedTransactionMemento.
func (x *CreatedTransactionMemento) MarshalJSON() ([]byte, error) {
	// Convert account metadata to map[string]map[string]string for JSON
	accountMeta := make(map[string]map[string]string)
	for k, v := range x.GetAccountMetadata() {
		accountMeta[k] = commonpb.MetadataSetToMap(v)
	}

	return json.Marshal(&struct {
		Transaction     *TransactionResume           `json:"transaction,omitempty"`
		AccountMetadata map[string]map[string]string `json:"accountMetadata,omitempty"`
	}{
		Transaction:     x.GetTransaction(),
		AccountMetadata: accountMeta,
	})
}

// MarshalJSON implements json.Marshaler for RevertedTransactionMemento.
func (x *RevertedTransactionMemento) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		RevertedTransactionId uint64             `json:"revertedTransactionId,omitempty"`
		RevertTransaction     *TransactionResume `json:"revertTransaction,omitempty"`
	}{
		RevertedTransactionId: x.GetRevertedTransactionId(),
		RevertTransaction:     x.GetRevertTransaction(),
	})
}
