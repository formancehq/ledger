package ledgerpb

import (
	"encoding/json"
)

// Note: Transaction.MarshalJSON is already implemented in transaction.go

// MarshalJSON implements json.Marshaler for PostCommitVolumes
func (x *PostCommitVolumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		VolumesByAccount map[string]*VolumesByAssets `json:"volumesByAccount,omitempty"`
	}{
		VolumesByAccount: x.VolumesByAccount,
	})
}

// MarshalJSON implements json.Marshaler for VolumesWithBalanceByAssetByAccount
func (x *VolumesWithBalanceByAssetByAccount) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Account            string              `json:"account,omitempty"`
		Asset              string              `json:"asset,omitempty"`
		VolumesWithBalance *VolumesWithBalance `json:"volumesWithBalance,omitempty"`
	}{
		Account:            x.Account,
		Asset:              x.Asset,
		VolumesWithBalance: x.VolumesWithBalance,
	})
}

// MarshalJSON implements json.Marshaler for Account
func (x *Account) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Address       string            `json:"address,omitempty"`
		Metadata      map[string]string `json:"metadata,omitempty"`
		FirstUsage    *Timestamp        `json:"firstUsage,omitempty"`
		InsertionDate *Timestamp        `json:"insertionDate,omitempty"`
		UpdatedAt     *Timestamp        `json:"updatedAt,omitempty"`
	}{
		Address:       x.Address,
		Metadata:      x.Metadata,
		FirstUsage:    x.FirstUsage,
		InsertionDate: x.InsertionDate,
		UpdatedAt:     x.UpdatedAt,
	})
}

// MarshalJSON implements json.Marshaler for Parameters
func (x *Parameters) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Ledger         string `json:"ledger,omitempty"`
		DryRun         bool   `json:"dryRun,omitempty"`
		IdempotencyKey string `json:"idempotencyKey,omitempty"`
	}{
		Ledger:         x.Ledger,
		DryRun:         x.DryRun,
		IdempotencyKey: x.IdempotencyKey,
	})
}

// MarshalJSON implements json.Marshaler for CreateTransactionRequestPayload
func (x *CreateTransactionRequestPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		AccountMetadata map[string]*Metadata `json:"accountMetadata,omitempty"`
		Metadata        map[string]string    `json:"metadata,omitempty"`
		Timestamp       *Timestamp           `json:"timestamp,omitempty"`
		Reference       string               `json:"reference,omitempty"`
		Postings        []*Posting           `json:"postings,omitempty"`
		Script          *Script              `json:"script,omitempty"`
		Runtime         string               `json:"runtime,omitempty"`
	}{
		AccountMetadata: x.AccountMetadata,
		Metadata:        x.Metadata,
		Timestamp:       x.Timestamp,
		Reference:       x.Reference,
		Postings:        x.Postings,
		Script:          x.Script,
		Runtime:         x.Runtime,
	})
}

// MarshalJSON implements json.Marshaler for CreateTransactionResponse
func (x *CreateTransactionResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction     *Transaction         `json:"transaction,omitempty"`
		AccountMetadata map[string]*Metadata `json:"accountMetadata,omitempty"`
	}{
		Transaction:     x.Transaction,
		AccountMetadata: x.AccountMetadata,
	})
}

// MarshalJSON implements json.Marshaler for RevertTransactionRequestPayload
func (x *RevertTransactionRequestPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TransactionId   uint64            `json:"transactionId,omitempty"`
		Force           bool              `json:"force,omitempty"`
		AtEffectiveDate bool              `json:"atEffectiveDate,omitempty"`
		Metadata        map[string]string `json:"metadata,omitempty"`
	}{
		TransactionId:   x.TransactionId,
		Force:           x.Force,
		AtEffectiveDate: x.AtEffectiveDate,
		Metadata:        x.Metadata,
	})
}

// MarshalJSON implements json.Marshaler for RevertTransactionResponse
func (x *RevertTransactionResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Log                 *Log                 `json:"log,omitempty"`
		RevertedTransaction *RevertedTransaction `json:"revertedTransaction,omitempty"`
	}{
		Log:                 x.Log,
		RevertedTransaction: x.RevertedTransaction,
	})
}

// MarshalJSON implements json.Marshaler for SaveTransactionMetadataRequestPayload
func (x *SaveTransactionMetadataRequestPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TransactionId uint64            `json:"transactionId,omitempty"`
		Metadata      map[string]string `json:"metadata,omitempty"`
	}{
		TransactionId: x.TransactionId,
		Metadata:      x.Metadata,
	})
}

// MarshalJSON implements json.Marshaler for DeleteTransactionMetadataRequestPayload
func (x *DeleteTransactionMetadataRequestPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TransactionId uint64 `json:"transactionId,omitempty"`
		Key           string `json:"key,omitempty"`
	}{
		TransactionId: x.TransactionId,
		Key:           x.Key,
	})
}

// MarshalJSON implements json.Marshaler for StreamLogsRequest
func (x *StreamLogsRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Ledger string `json:"ledger,omitempty"`
		FromId uint64 `json:"fromId,omitempty"`
		ToId   uint64 `json:"toId,omitempty"`
	}{
		Ledger: x.Ledger,
		FromId: x.FromId,
		ToId:   x.ToId,
	})
}

// Note: Log.MarshalJSON is already implemented in log.go

// MarshalJSON implements json.Marshaler for CreatedTransaction
func (x *CreatedTransaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction     *Transaction         `json:"transaction,omitempty"`
		AccountMetadata map[string]*Metadata `json:"accountMetadata,omitempty"`
	}{
		Transaction:     x.Transaction,
		AccountMetadata: x.AccountMetadata,
	})
}

// MarshalJSON implements json.Marshaler for RevertedTransaction
func (x *RevertedTransaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		RevertedTransaction *Transaction `json:"revertedTransaction,omitempty"`
		RevertTransaction   *Transaction `json:"revertTransaction,omitempty"`
	}{
		RevertedTransaction: x.RevertedTransaction,
		RevertTransaction:   x.RevertTransaction,
	})
}

// MarshalJSON implements json.Marshaler for SavedMetadata
func (x *SavedMetadata) MarshalJSON() ([]byte, error) {
	aux := &struct {
		TargetType    string            `json:"targetType,omitempty"`
		AccountId     string            `json:"accountId,omitempty"`
		TransactionId uint64            `json:"transactionId,omitempty"`
		Metadata      map[string]string `json:"metadata,omitempty"`
	}{
		TargetType: x.TargetType,
		Metadata:   x.Metadata,
	}

	// Handle oneof target_id
	if x.TargetId != nil {
		switch v := x.TargetId.(type) {
		case *SavedMetadata_AccountId:
			aux.AccountId = v.AccountId
		case *SavedMetadata_TransactionId:
			aux.TransactionId = v.TransactionId
		}
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for DeletedMetadata
func (x *DeletedMetadata) MarshalJSON() ([]byte, error) {
	aux := &struct {
		TargetType    string `json:"targetType,omitempty"`
		AccountId     string `json:"accountId,omitempty"`
		TransactionId uint64 `json:"transactionId,omitempty"`
		Key           string `json:"key,omitempty"`
	}{
		TargetType: x.TargetType,
		Key:        x.Key,
	}

	// Handle oneof target_id
	if x.TargetId != nil {
		switch v := x.TargetId.(type) {
		case *DeletedMetadata_AccountId:
			aux.AccountId = v.AccountId
		case *DeletedMetadata_TransactionId:
			aux.TransactionId = v.TransactionId
		}
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for CreatedTransactionMemento
func (x *CreatedTransactionMemento) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction     *TransactionResume   `json:"transaction,omitempty"`
		AccountMetadata map[string]*Metadata `json:"accountMetadata,omitempty"`
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

// MarshalJSON implements json.Marshaler for LedgerInfo
func (x *LedgerInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Id                uint64            `json:"id,omitempty"`
		Name              string            `json:"name,omitempty"`
		Driver            string            `json:"driver,omitempty"`
		Config            interface{}       `json:"config,omitempty"`
		Metadata          map[string]string `json:"metadata,omitempty"`
		CreatedAt         *Timestamp        `json:"createdAt,omitempty"`
		SnapshotThreshold uint64            `json:"snapshotThreshold,omitempty"`
		DeletedAt         *Timestamp        `json:"deletedAt,omitempty"`
	}{
		Id:                x.Id,
		Name:              x.Name,
		Driver:            x.Driver,
		Config:            x.Config,
		Metadata:          x.Metadata,
		CreatedAt:         x.CreatedAt,
		SnapshotThreshold: x.SnapshotThreshold,
		DeletedAt:         x.DeletedAt,
	})
}
