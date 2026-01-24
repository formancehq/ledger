package ledgerpb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/json"
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

// MarshalJSON implements json.Marshaler for CreateTransactionRequestPayload
func (x *CreateTransactionRequestPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		AccountMetadata map[string]*Metadata `json:"accountMetadata,omitempty"`
		Metadata        map[string]string    `json:"metadata,omitempty"`
		Timestamp       *Timestamp           `json:"timestamp,omitempty"`
		Reference       string               `json:"reference,omitempty"`
		Postings        []*Posting           `json:"postings,omitempty"`
		Script          *Script              `json:"script,omitempty"`
	}{
		AccountMetadata: x.AccountMetadata,
		Metadata:        x.Metadata,
		Timestamp:       x.Timestamp,
		Reference:       x.Reference,
		Postings:        x.Postings,
		Script:          x.Script,
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

// MarshalJSON implements json.Marshaler for SaveTransactionMetadataRequestPayload
func (x *SaveTransactionMetadataRequestPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TransactionId uint64            `json:"transactionId,omitempty"`
		Metadata      map[string]string `json:"metadata,omitempty"`
	}{
		TransactionId: x.TransactionId,
		Metadata:      x.Metadata.Entries,
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
		RevertedTransactionID uint64       `json:"revertedTransactionID,omitempty"`
		RevertTransaction     *Transaction `json:"revertTransaction,omitempty"`
	}{
		RevertedTransactionID: x.RevertedTransactionId,
		RevertTransaction:     x.RevertTransaction,
	})
}

// MarshalJSON implements json.Marshaler for SavedMetadata
func (x *SavedMetadata) MarshalJSON() ([]byte, error) {
	aux := struct {
		TargetType    string            `json:"targetType,omitempty"`
		AccountId     string            `json:"accountId,omitempty"`
		TransactionId uint64            `json:"transactionId,omitempty"`
		Metadata      map[string]string `json:"metadata,omitempty"`
	}{
		TargetType: x.Target.AsConst(),
		Metadata:   x.Metadata.Entries,
	}

	// Handle oneof target_id
	switch v := x.Target.Target.(type) {
	case *Target_Account:
		aux.AccountId = v.Account.Addr
	case *Target_Transaction:
		aux.TransactionId = v.Transaction.Id
	}

	return json.Marshal(aux)
}

// MarshalJSON implements json.Marshaler for DeletedMetadata
func (x *DeletedMetadata) MarshalJSON() ([]byte, error) {
	aux := struct {
		TargetType    string `json:"targetType,omitempty"`
		AccountId     string `json:"accountId,omitempty"`
		TransactionId uint64 `json:"transactionId,omitempty"`
		Key           string `json:"key,omitempty"`
	}{
		TargetType: x.Target.AsConst(),
		Key:        x.Key,
	}

	// Handle oneof target_id
	switch v := x.Target.Target.(type) {
	case *Target_Account:
		aux.AccountId = v.Account.Addr
	case *Target_Transaction:
		aux.TransactionId = v.Transaction.Id
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

// UnmarshalJSON implements json.Unmarshaler for DeletedMetadata
// Handles the special case where TargetID can be either a string (for ACCOUNT) or uint64 (for TRANSACTION)
func (dm *DeletedMetadata) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string        `json:"targetType"`
		TargetID   json.RawValue `json:"targetId"`
		Key        string        `json:"key"`
	}
	x := X{}
	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}

	dm.Key = x.Key

	switch strings.ToUpper(x.TargetType) {
	case strings.ToUpper(MetaTargetTypeAccount):
		var accountID string
		err = json.Unmarshal(x.TargetID, &accountID)
		if err == nil {
			dm.Target = &Target{
				Target: &Target_Account{
					Account: &TargetAccount{
						Addr: accountID,
					},
				},
			}
		}
	case strings.ToUpper(MetaTargetTypeTransaction):
		var txID uint64
		txID, err = strconv.ParseUint(string(x.TargetID), 10, 64)
		if err == nil {
			dm.Target = &Target{
				Target: &Target_Transaction{
					Transaction: &TargetTransaction{
						Id: txID,
					},
				},
			}
		}
	default:
		return fmt.Errorf("unknown type '%s'", x.TargetType)
	}
	return err
}

// UnmarshalJSON implements json.Unmarshaler for SavedMetadata
// Handles the special case where TargetID can be either a string (for ACCOUNT) or uint64 (for TRANSACTION)
func (sm *SavedMetadata) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string            `json:"targetType"`
		TargetID   json.RawValue     `json:"targetId"`
		Metadata   metadata.Metadata `json:"metadata"`
	}
	x := X{}
	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}

	sm.Metadata = &Metadata{
		Entries: x.Metadata,
	}

	switch strings.ToUpper(x.TargetType) {
	case strings.ToUpper(MetaTargetTypeAccount):
		var accountID string
		err = json.Unmarshal(x.TargetID, &accountID)
		if err == nil {
			sm.Target = &Target{
				Target: &Target_Account{
					Account: &TargetAccount{
						Addr: accountID,
					},
				},
			}
		}
	case strings.ToUpper(MetaTargetTypeTransaction):
		var txID uint64
		txID, err = strconv.ParseUint(string(x.TargetID), 10, 64)
		if err == nil {
			sm.Target = &Target{
				Target: &Target_Transaction{
					Transaction: &TargetTransaction{
						Id: txID,
					},
				},
			}
		}
	default:
		return fmt.Errorf("unknown type '%s'", x.TargetType)
	}
	return err
}

func (state *LedgerState) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		LedgerInfo        *LedgerInfo `json:"ledgerInfo,omitempty"`
		NextLogId         uint64      `json:"nextLogId,omitempty"`
		NextTransactionId uint64      `json:"nextTransactionId,omitempty"`
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
