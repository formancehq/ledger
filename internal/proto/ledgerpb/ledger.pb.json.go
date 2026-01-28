package ledgerpb

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// MarshalJSON implements json.Marshaler for CreateTransactionRequestPayload
func (x *CreateTransactionRequestPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		AccountMetadata map[string]*commonpb.Metadata `json:"accountMetadata,omitempty"`
		Metadata        map[string]string             `json:"metadata,omitempty"`
		Timestamp       *commonpb.Timestamp           `json:"timestamp,omitempty"`
		Reference       string                        `json:"reference,omitempty"`
		Postings        []*commonpb.Posting           `json:"postings,omitempty"`
		Script          *commonpb.Script              `json:"script,omitempty"`
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

// Ledger action type constants
const (
	LedgerActionTypeCreateTransaction = "CREATE_TRANSACTION"
	LedgerActionTypeAddMetadata       = "ADD_METADATA"
	LedgerActionTypeRevertTransaction = "REVERT_TRANSACTION"
	LedgerActionTypeDeleteMetadata    = "DELETE_METADATA"
)

// UnmarshalJSON implements json.Unmarshaler for LedgerAction
func (x *LedgerAction) UnmarshalJSON(data []byte) error {
	// First pass: parse action and idempotency key
	type rawElement struct {
		Action         string        `json:"action"`
		IdempotencyKey string        `json:"ik"`
		Data           json.RawValue `json:"data"`
	}
	var raw rawElement
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("error parsing element: %w", err)
	}

	x.IdempotencyKey = raw.IdempotencyKey

	// Parse data based on action
	switch raw.Action {
	case LedgerActionTypeCreateTransaction:
		req := &CreateTransactionRequestPayload{}
		if err := json.Unmarshal(raw.Data, req); err != nil {
			return fmt.Errorf("error parsing create transaction data: %w", err)
		}
		x.Data = &LedgerAction_CreateTransaction{CreateTransaction: req}

	case LedgerActionTypeAddMetadata:
		req, err := unmarshalSaveMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing add metadata data: %w", err)
		}
		x.Data = &LedgerAction_AddMetadata{AddMetadata: req}

	case LedgerActionTypeRevertTransaction:
		req, err := unmarshalRevertTransactionRequestPayload(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing revert transaction data: %w", err)
		}
		x.Data = &LedgerAction_RevertTransaction{RevertTransaction: req}

	case LedgerActionTypeDeleteMetadata:
		req, err := unmarshalDeleteMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing delete metadata data: %w", err)
		}
		x.Data = &LedgerAction_DeleteMetadata{DeleteMetadata: req}

	default:
		return fmt.Errorf("unsupported action: %s", raw.Action)
	}

	return nil
}

// GetLedgerActionType returns the action type string based on the oneof data
func GetLedgerActionType(action *LedgerAction) string {
	switch action.Data.(type) {
	case *LedgerAction_CreateTransaction:
		return LedgerActionTypeCreateTransaction
	case *LedgerAction_AddMetadata:
		return LedgerActionTypeAddMetadata
	case *LedgerAction_RevertTransaction:
		return LedgerActionTypeRevertTransaction
	case *LedgerAction_DeleteMetadata:
		return LedgerActionTypeDeleteMetadata
	default:
		return ""
	}
}

// unmarshalSaveMetadataCommand unmarshals JSON into SaveMetadataCommand
func unmarshalSaveMetadataCommand(data json.RawValue) (*commonpb.SaveMetadataCommand, error) {
	type rawReq struct {
		TargetType string            `json:"targetType"`
		TargetID   json.RawValue     `json:"targetId"`
		Metadata   map[string]string `json:"metadata"`
	}
	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	return &commonpb.SaveMetadataCommand{
		Target:   commonpb.ParseTarget(raw.TargetType, raw.TargetID),
		Metadata: &commonpb.Metadata{Entries: raw.Metadata},
	}, nil
}

// unmarshalRevertTransactionRequestPayload unmarshals JSON into RevertTransactionRequestPayload
func unmarshalRevertTransactionRequestPayload(data json.RawValue) (*RevertTransactionRequestPayload, error) {
	type rawReq struct {
		ID              uint64            `json:"id"`
		Force           bool              `json:"force"`
		AtEffectiveDate bool              `json:"atEffectiveDate"`
		Metadata        map[string]string `json:"metadata"`
	}
	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	return &RevertTransactionRequestPayload{
		TransactionId:   raw.ID,
		Force:           raw.Force,
		AtEffectiveDate: raw.AtEffectiveDate,
		Metadata:        raw.Metadata,
	}, nil
}

// unmarshalDeleteMetadataCommand unmarshals JSON into DeleteMetadataCommand
func unmarshalDeleteMetadataCommand(data json.RawValue) (*commonpb.DeleteMetadataCommand, error) {
	type rawReq struct {
		TargetType string        `json:"targetType"`
		TargetID   json.RawValue `json:"targetId"`
		Key        string        `json:"key"`
	}
	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	return &commonpb.DeleteMetadataCommand{
		Target: commonpb.ParseTarget(raw.TargetType, raw.TargetID),
		Key:    raw.Key,
	}, nil
}

// parseTarget parses targetType and targetId into a Target message
// Deprecated: use commonpb.ParseTarget instead
func parseTarget(targetType string, targetID json.RawValue) *commonpb.Target {
	if len(targetID) == 0 {
		return nil
	}

	switch strings.ToUpper(targetType) {
	case commonpb.MetaTargetTypeAccount:
		var addr string
		if err := json.Unmarshal(targetID, &addr); err == nil {
			return &commonpb.Target{
				Target: &commonpb.Target_Account{
					Account: &commonpb.TargetAccount{Addr: addr},
				},
			}
		}
	case commonpb.MetaTargetTypeTransaction:
		var id uint64
		if err := json.Unmarshal(targetID, &id); err == nil {
			return &commonpb.Target{
				Target: &commonpb.Target_Transaction{
					Transaction: &commonpb.TargetTransaction{Id: id},
				},
			}
		}
	}

	return nil
}
