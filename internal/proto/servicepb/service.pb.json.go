package servicepb

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// MarshalJSON implements json.Marshaler for CreateTransactionPayload
func (x *CreateTransactionPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		AccountMetadata map[string]map[string]any `json:"accountMetadata,omitempty"`
		Metadata        map[string]any            `json:"metadata,omitempty"`
		Timestamp       *commonpb.Timestamp       `json:"timestamp,omitempty"`
		Reference       string                    `json:"reference,omitempty"`
		Postings        []*commonpb.Posting       `json:"postings,omitempty"`
		Script          *commonpb.Script          `json:"script,omitempty"`
	}{
		AccountMetadata: commonpb.AccountMetadataToAnyMap(x.AccountMetadata),
		Metadata:        commonpb.MetadataSetToAnyMap(x.Metadata),
		Timestamp:       x.Timestamp,
		Reference:       x.Reference,
		Postings:        x.Postings,
		Script:          x.Script,
	})
}

// MarshalJSON implements json.Marshaler for RevertTransactionPayload
func (x *RevertTransactionPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TransactionId   uint64         `json:"transactionId,omitempty"`
		Force           bool           `json:"force,omitempty"`
		AtEffectiveDate bool           `json:"atEffectiveDate,omitempty"`
		Metadata        map[string]any `json:"metadata,omitempty"`
	}{
		TransactionId:   x.TransactionId,
		Force:           x.Force,
		AtEffectiveDate: x.AtEffectiveDate,
		Metadata:        commonpb.MetadataSetToAnyMap(x.Metadata),
	})
}

// Ledger action type constants
const (
	LedgerActionTypeCreateTransaction = "CREATE_TRANSACTION"
	LedgerActionTypeAddMetadata       = "ADD_METADATA"
	LedgerActionTypeRevertTransaction = "REVERT_TRANSACTION"
	LedgerActionTypeDeleteMetadata    = "DELETE_METADATA"
)

// BulkElement represents a bulk element with idempotency key
type BulkElement struct {
	Action         *LedgerApplyRequest
	IdempotencyKey string
}

// UnmarshalJSON implements json.Unmarshaler for BulkElement
func (x *BulkElement) UnmarshalJSON(data []byte) error {
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
	x.Action = &LedgerApplyRequest{}

	// Parse data based on action
	switch raw.Action {
	case LedgerActionTypeCreateTransaction:
		req := &CreateTransactionPayload{}
		if err := json.Unmarshal(raw.Data, req); err != nil {
			return fmt.Errorf("error parsing create transaction data: %w", err)
		}
		x.Action.Data = &LedgerApplyRequest_CreateTransaction{CreateTransaction: req}

	case LedgerActionTypeAddMetadata:
		req, err := unmarshalSaveMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing add metadata data: %w", err)
		}
		x.Action.Data = &LedgerApplyRequest_AddMetadata{AddMetadata: req}

	case LedgerActionTypeRevertTransaction:
		req, err := unmarshalRevertTransactionPayload(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing revert transaction data: %w", err)
		}
		x.Action.Data = &LedgerApplyRequest_RevertTransaction{RevertTransaction: req}

	case LedgerActionTypeDeleteMetadata:
		req, err := unmarshalDeleteMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing delete metadata data: %w", err)
		}
		x.Action.Data = &LedgerApplyRequest_DeleteMetadata{DeleteMetadata: req}

	default:
		return fmt.Errorf("unsupported action: %s", raw.Action)
	}

	return nil
}

// UnmarshalJSON implements json.Unmarshaler for LedgerApplyRequest
func (x *LedgerApplyRequest) UnmarshalJSON(data []byte) error {
	// First pass: parse action
	type rawElement struct {
		Action string        `json:"action"`
		Data   json.RawValue `json:"data"`
	}
	var raw rawElement
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("error parsing element: %w", err)
	}

	// Parse data based on action
	switch raw.Action {
	case LedgerActionTypeCreateTransaction:
		req := &CreateTransactionPayload{}
		if err := json.Unmarshal(raw.Data, req); err != nil {
			return fmt.Errorf("error parsing create transaction data: %w", err)
		}
		x.Data = &LedgerApplyRequest_CreateTransaction{CreateTransaction: req}

	case LedgerActionTypeAddMetadata:
		req, err := unmarshalSaveMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing add metadata data: %w", err)
		}
		x.Data = &LedgerApplyRequest_AddMetadata{AddMetadata: req}

	case LedgerActionTypeRevertTransaction:
		req, err := unmarshalRevertTransactionPayload(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing revert transaction data: %w", err)
		}
		x.Data = &LedgerApplyRequest_RevertTransaction{RevertTransaction: req}

	case LedgerActionTypeDeleteMetadata:
		req, err := unmarshalDeleteMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing delete metadata data: %w", err)
		}
		x.Data = &LedgerApplyRequest_DeleteMetadata{DeleteMetadata: req}

	default:
		return fmt.Errorf("unsupported action: %s", raw.Action)
	}

	return nil
}

// GetLedgerApplyActionType returns the action type string based on the oneof data
func GetLedgerApplyActionType(action *LedgerApplyRequest) string {
	switch action.Data.(type) {
	case *LedgerApplyRequest_CreateTransaction:
		return LedgerActionTypeCreateTransaction
	case *LedgerApplyRequest_AddMetadata:
		return LedgerActionTypeAddMetadata
	case *LedgerApplyRequest_RevertTransaction:
		return LedgerActionTypeRevertTransaction
	case *LedgerApplyRequest_DeleteMetadata:
		return LedgerActionTypeDeleteMetadata
	default:
		return ""
	}
}

// unmarshalSaveMetadataCommand unmarshals JSON into SaveMetadataCommand
func unmarshalSaveMetadataCommand(data json.RawValue) (*commonpb.SaveMetadataCommand, error) {
	type rawReq struct {
		TargetType string         `json:"targetType"`
		TargetID   json.RawValue  `json:"targetId"`
		Metadata   map[string]any `json:"metadata"`
	}
	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	ms, err := commonpb.MetadataSetFromAnyMap(raw.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	return &commonpb.SaveMetadataCommand{
		Target:   commonpb.ParseTarget(raw.TargetType, raw.TargetID),
		Metadata: ms,
	}, nil
}

// unmarshalRevertTransactionPayload unmarshals JSON into RevertTransactionPayload
func unmarshalRevertTransactionPayload(data json.RawValue) (*RevertTransactionPayload, error) {
	type rawReq struct {
		ID              uint64         `json:"id"`
		Force           bool           `json:"force"`
		AtEffectiveDate bool           `json:"atEffectiveDate"`
		Metadata        map[string]any `json:"metadata"`
	}
	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	ms, err := commonpb.MetadataSetFromAnyMap(raw.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	return &RevertTransactionPayload{
		TransactionId:   raw.ID,
		Force:           raw.Force,
		AtEffectiveDate: raw.AtEffectiveDate,
		Metadata:        ms,
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
