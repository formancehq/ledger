package servicepb

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// MarshalJSON implements json.Marshaler for CreateTransactionPayload
func (x *CreateTransactionPayload) MarshalJSON() ([]byte, error) {
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

// MarshalJSON implements json.Marshaler for RevertTransactionPayload
func (x *RevertTransactionPayload) MarshalJSON() ([]byte, error) {
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

// Ledger action type constants
const (
	LedgerActionTypeCreateTransaction = "CREATE_TRANSACTION"
	LedgerActionTypeAddMetadata       = "ADD_METADATA"
	LedgerActionTypeRevertTransaction = "REVERT_TRANSACTION"
	LedgerActionTypeDeleteMetadata    = "DELETE_METADATA"
)

// LedgerID creates a LedgerNameOrId with an ID
func LedgerID(id uint32) *LedgerNameOrId {
	return &LedgerNameOrId{
		Type: &LedgerNameOrId_Id{Id: id},
	}
}

// LedgerName creates a LedgerNameOrId with a name
func LedgerName(name string) *LedgerNameOrId {
	return &LedgerNameOrId{
		Type: &LedgerNameOrId_Name{Name: name},
	}
}

// UnmarshalJSON implements json.Unmarshaler for LedgerApplyAction
func (x *LedgerApplyAction) UnmarshalJSON(data []byte) error {
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
		req := &CreateTransactionPayload{}
		if err := json.Unmarshal(raw.Data, req); err != nil {
			return fmt.Errorf("error parsing create transaction data: %w", err)
		}
		x.Data = &LedgerApplyAction_CreateTransaction{CreateTransaction: req}

	case LedgerActionTypeAddMetadata:
		req, err := unmarshalSaveMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing add metadata data: %w", err)
		}
		x.Data = &LedgerApplyAction_AddMetadata{AddMetadata: req}

	case LedgerActionTypeRevertTransaction:
		req, err := unmarshalRevertTransactionPayload(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing revert transaction data: %w", err)
		}
		x.Data = &LedgerApplyAction_RevertTransaction{RevertTransaction: req}

	case LedgerActionTypeDeleteMetadata:
		req, err := unmarshalDeleteMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing delete metadata data: %w", err)
		}
		x.Data = &LedgerApplyAction_DeleteMetadata{DeleteMetadata: req}

	default:
		return fmt.Errorf("unsupported action: %s", raw.Action)
	}

	return nil
}

// GetLedgerApplyActionType returns the action type string based on the oneof data
func GetLedgerApplyActionType(action *LedgerApplyAction) string {
	switch action.Data.(type) {
	case *LedgerApplyAction_CreateTransaction:
		return LedgerActionTypeCreateTransaction
	case *LedgerApplyAction_AddMetadata:
		return LedgerActionTypeAddMetadata
	case *LedgerApplyAction_RevertTransaction:
		return LedgerActionTypeRevertTransaction
	case *LedgerApplyAction_DeleteMetadata:
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

// unmarshalRevertTransactionPayload unmarshals JSON into RevertTransactionPayload
func unmarshalRevertTransactionPayload(data json.RawValue) (*RevertTransactionPayload, error) {
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

	return &RevertTransactionPayload{
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
