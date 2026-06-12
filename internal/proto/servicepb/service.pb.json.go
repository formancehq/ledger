package servicepb

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// MarshalJSON implements json.Marshaler for GetTransactionResponse.
func (x *GetTransactionResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction *commonpb.Transaction `json:"transaction,omitempty"`
		Receipt     string                `json:"receipt,omitempty"`
	}{
		Transaction: x.GetTransaction(),
		Receipt:     x.GetReceipt(),
	})
}

// MarshalJSON implements json.Marshaler for CreateTransactionPayload.
func (x *CreateTransactionPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		AccountMetadata map[string]map[string]any `json:"accountMetadata,omitempty"`
		Metadata        map[string]any            `json:"metadata,omitempty"`
		Timestamp       *commonpb.Timestamp       `json:"timestamp,omitempty"`
		Reference       string                    `json:"reference,omitempty"`
		Postings        []*commonpb.Posting       `json:"postings,omitempty"`
		Script          *commonpb.Script          `json:"script,omitempty"`
	}{
		AccountMetadata: commonpb.AccountMetadataToAnyMap(x.GetAccountMetadata()),
		Metadata:        commonpb.MetadataToAnyMap(x.GetMetadata()),
		Timestamp:       x.GetTimestamp(),
		Reference:       x.GetReference(),
		Postings:        x.GetPostings(),
		Script:          x.GetScript(),
	})
}

// MarshalJSON implements json.Marshaler for RevertTransactionPayload.
// Exactly one of the identifier variants is emitted: transactionId for the
// numeric id, transactionReference for the reference. Both carry omitempty so
// only the variant actually set appears in the output.
func (x *RevertTransactionPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TransactionId        uint64         `json:"transactionId,omitempty"`
		TransactionReference string         `json:"transactionReference,omitempty"`
		Force                bool           `json:"force,omitempty"`
		AtEffectiveDate      bool           `json:"atEffectiveDate,omitempty"`
		Metadata             map[string]any `json:"metadata,omitempty"`
	}{
		TransactionId:        x.GetTransactionId(),
		TransactionReference: x.GetTransactionReference(),
		Force:                x.GetForce(),
		AtEffectiveDate:      x.GetAtEffectiveDate(),
		Metadata:             commonpb.MetadataToAnyMap(x.GetMetadata()),
	})
}

// Ledger action type constants.
const (
	LedgerActionTypeCreateTransaction = "CREATE_TRANSACTION"
	LedgerActionTypeAddMetadata       = "ADD_METADATA"
	LedgerActionTypeRevertTransaction = "REVERT_TRANSACTION"
	LedgerActionTypeDeleteMetadata    = "DELETE_METADATA"
)

// BulkElement represents a bulk element with idempotency key.
type BulkElement struct {
	Action         *LedgerAction
	IdempotencyKey string
}

// UnmarshalJSON implements json.Unmarshaler for BulkElement.
func (x *BulkElement) UnmarshalJSON(data []byte) error {
	// First pass: parse action and idempotency key
	type rawElement struct {
		Action         string        `json:"action"`
		IdempotencyKey string        `json:"ik"`
		Data           json.RawValue `json:"data"`
	}

	var raw rawElement

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return fmt.Errorf("error parsing element: %w", err)
	}

	x.IdempotencyKey = raw.IdempotencyKey
	x.Action = &LedgerAction{}

	// Parse data based on action
	switch raw.Action {
	case LedgerActionTypeCreateTransaction:
		req := &CreateTransactionPayload{}

		err := json.Unmarshal(raw.Data, req)
		if err != nil {
			return fmt.Errorf("error parsing create transaction data: %w", err)
		}

		x.Action.Data = &LedgerAction_CreateTransaction{CreateTransaction: req}

	case LedgerActionTypeAddMetadata:
		req, err := unmarshalSaveMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing add metadata data: %w", err)
		}

		x.Action.Data = &LedgerAction_AddMetadata{AddMetadata: req}

	case LedgerActionTypeRevertTransaction:
		req, err := unmarshalRevertTransactionPayload(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing revert transaction data: %w", err)
		}

		x.Action.Data = &LedgerAction_RevertTransaction{RevertTransaction: req}

	case LedgerActionTypeDeleteMetadata:
		req, err := unmarshalDeleteMetadataCommand(raw.Data)
		if err != nil {
			return fmt.Errorf("error parsing delete metadata data: %w", err)
		}

		x.Action.Data = &LedgerAction_DeleteMetadata{DeleteMetadata: req}

	default:
		return fmt.Errorf("unsupported action: %s", raw.Action)
	}

	return nil
}

// UnmarshalJSON implements json.Unmarshaler for LedgerAction.
func (x *LedgerAction) UnmarshalJSON(data []byte) error {
	// First pass: parse action
	type rawElement struct {
		Action string        `json:"action"`
		Data   json.RawValue `json:"data"`
	}

	var raw rawElement

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return fmt.Errorf("error parsing element: %w", err)
	}

	// Parse data based on action
	switch raw.Action {
	case LedgerActionTypeCreateTransaction:
		req := &CreateTransactionPayload{}

		err := json.Unmarshal(raw.Data, req)
		if err != nil {
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
		req, err := unmarshalRevertTransactionPayload(raw.Data)
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

// GetLedgerActionType returns the action type string based on the oneof data.
func GetLedgerActionType(action *LedgerAction) string {
	switch action.GetData().(type) {
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

// unmarshalSaveMetadataCommand unmarshals JSON into SaveMetadataCommand.
func unmarshalSaveMetadataCommand(data json.RawValue) (*commonpb.SaveMetadataCommand, error) {
	type rawReq struct {
		TargetType      string         `json:"targetType"`
		TargetID        json.RawValue  `json:"targetId"`
		TargetReference string         `json:"targetReference"`
		Metadata        map[string]any `json:"metadata"`
	}

	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	ms, err := commonpb.MetadataFromAnyMap(raw.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	target, err := commonpb.ParseTarget(raw.TargetType, raw.TargetID, raw.TargetReference)
	if err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	return &commonpb.SaveMetadataCommand{
		Target:   target,
		Metadata: ms,
	}, nil
}

// unmarshalRevertTransactionPayload unmarshals JSON into RevertTransactionPayload.
// Exactly one of id or reference must be set; when both are present, reference wins.
func unmarshalRevertTransactionPayload(data json.RawValue) (*RevertTransactionPayload, error) {
	type rawReq struct {
		ID              uint64         `json:"id"`
		Reference       string         `json:"reference"`
		Force           bool           `json:"force"`
		AtEffectiveDate bool           `json:"atEffectiveDate"`
		Metadata        map[string]any `json:"metadata"`
	}

	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	ms, err := commonpb.MetadataFromAnyMap(raw.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	var identifier isRevertTransactionPayload_Identifier

	switch {
	case raw.Reference != "" && raw.ID != 0:
		return nil, errors.New("revert payload must set either id or reference, not both")
	case raw.Reference != "":
		identifier = &RevertTransactionPayload_TransactionReference{TransactionReference: raw.Reference}
	case raw.ID != 0:
		identifier = &RevertTransactionPayload_TransactionId{TransactionId: raw.ID}
	default:
		return nil, errors.New("revert payload requires either id or reference")
	}

	return &RevertTransactionPayload{
		Identifier:      identifier,
		Force:           raw.Force,
		AtEffectiveDate: raw.AtEffectiveDate,
		Metadata:        ms,
	}, nil
}

// unmarshalDeleteMetadataCommand unmarshals JSON into DeleteMetadataCommand.
func unmarshalDeleteMetadataCommand(data json.RawValue) (*commonpb.DeleteMetadataCommand, error) {
	type rawReq struct {
		TargetType      string        `json:"targetType"`
		TargetID        json.RawValue `json:"targetId"`
		TargetReference string        `json:"targetReference"`
		Key             string        `json:"key"`
	}

	var raw rawReq

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}

	target, err := commonpb.ParseTarget(raw.TargetType, raw.TargetID, raw.TargetReference)
	if err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	return &commonpb.DeleteMetadataCommand{
		Target: target,
		Key:    raw.Key,
	}, nil
}
