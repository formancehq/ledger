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
		AccountMetadata  map[string]map[string]any `json:"accountMetadata,omitempty"`
		Metadata         map[string]any            `json:"metadata,omitempty"`
		Timestamp        *commonpb.Timestamp       `json:"timestamp,omitempty"`
		Reference        string                    `json:"reference,omitempty"`
		Postings         []*commonpb.Posting       `json:"postings,omitempty"`
		Script           *commonpb.Script          `json:"script,omitempty"`
		ScriptReference  *ScriptReference          `json:"scriptReference,omitempty"`
		SkippableReasons []string                  `json:"skippableReasons,omitempty"`
		Force            bool                      `json:"force,omitempty"`
		ExpandVolumes    bool                      `json:"expandVolumes,omitempty"`
	}{
		AccountMetadata:  commonpb.AccountMetadataToAnyMap(x.GetAccountMetadata()),
		Metadata:         commonpb.MetadataToAnyMap(x.GetMetadata()),
		Timestamp:        x.GetTimestamp(),
		Reference:        x.GetReference(),
		Postings:         x.GetPostings(),
		Script:           x.GetScript(),
		ScriptReference:  x.GetScriptReference(),
		SkippableReasons: errorReasonsToStrings(x.GetSkippableReasons()),
		Force:            x.GetForce(),
		ExpandVolumes:    x.GetExpandVolumes(),
	})
}

// UnmarshalJSON implements json.Unmarshaler for CreateTransactionPayload.
//
// The default protoc-gen-go struct tags are snake_case, so a plain
// encoding/json decode would silently drop the multi-word camelCase keys the
// REST contract advertises (scriptReference, accountMetadata, expandVolumes,
// …) and produce a zero-posting transaction (#452). We mirror MarshalJSON's
// shape here, then rebuild the protobuf struct field-by-field. Unknown JSON
// keys are tolerated to preserve the lenient behavior of the previous
// decoder.
func (x *CreateTransactionPayload) UnmarshalJSON(data []byte) error {
	var aux struct {
		AccountMetadata  map[string]map[string]any `json:"accountMetadata"`
		Metadata         map[string]any            `json:"metadata"`
		Timestamp        *commonpb.Timestamp       `json:"timestamp"`
		Reference        string                    `json:"reference"`
		Postings         []*commonpb.Posting       `json:"postings"`
		Script           *commonpb.Script          `json:"script"`
		ScriptReference  *ScriptReference          `json:"scriptReference"`
		SkippableReasons []string                  `json:"skippableReasons"`
		Force            bool                      `json:"force"`
		ExpandVolumes    bool                      `json:"expandVolumes"`
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	metadata, err := commonpb.MetadataFromAnyMap(aux.Metadata)
	if err != nil {
		return fmt.Errorf("invalid metadata: %w", err)
	}

	var accountMetadata map[string]*commonpb.MetadataMap
	if len(aux.AccountMetadata) > 0 {
		accountMetadata = make(map[string]*commonpb.MetadataMap, len(aux.AccountMetadata))

		for account, values := range aux.AccountMetadata {
			mv, err := commonpb.MetadataFromAnyMap(values)
			if err != nil {
				return fmt.Errorf("invalid account metadata for %q: %w", account, err)
			}

			accountMetadata[account] = &commonpb.MetadataMap{Values: mv}
		}
	}

	skippable, err := errorReasonsFromStrings(aux.SkippableReasons)
	if err != nil {
		return fmt.Errorf("invalid skippableReasons: %w", err)
	}

	x.Postings = aux.Postings
	x.Script = aux.Script
	x.Timestamp = aux.Timestamp
	x.Reference = aux.Reference
	x.Metadata = metadata
	x.AccountMetadata = accountMetadata
	x.Force = aux.Force
	x.ExpandVolumes = aux.ExpandVolumes
	x.ScriptReference = aux.ScriptReference
	x.SkippableReasons = skippable

	return nil
}

// errorReasonsToStrings serialises an ErrorReason slice as its public enum
// names ("ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT") so the JSON shape
// matches the OpenAPI contract advertised at openapi.yml#components.schemas.
// Returns nil on an empty slice so `omitempty` drops the field on the wire.
func errorReasonsToStrings(reasons []commonpb.ErrorReason) []string {
	if len(reasons) == 0 {
		return nil
	}

	out := make([]string, len(reasons))
	for i, r := range reasons {
		out[i] = r.String()
	}

	return out
}

// errorReasonsFromStrings is the inverse of errorReasonsToStrings: parses the
// enum-name list a REST caller submits. Unknown names fail loudly so a
// typo in `skippableReasons` is rejected at admission with a clear 400
// rather than silently dropped.
func errorReasonsFromStrings(in []string) ([]commonpb.ErrorReason, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make([]commonpb.ErrorReason, len(in))

	for i, name := range in {
		code, ok := commonpb.ErrorReason_value[name]
		if !ok {
			return nil, fmt.Errorf("unknown ErrorReason %q at index %d", name, i)
		}

		out[i] = commonpb.ErrorReason(code)
	}

	return out, nil
}

// MarshalJSON implements json.Marshaler for RevertTransactionPayload.
func (x *RevertTransactionPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		TransactionId   uint64         `json:"transactionId,omitempty"`
		Force           bool           `json:"force,omitempty"`
		AtEffectiveDate bool           `json:"atEffectiveDate,omitempty"`
		Metadata        map[string]any `json:"metadata,omitempty"`
		Receipt         string         `json:"receipt,omitempty"`
		ExpandVolumes   bool           `json:"expandVolumes,omitempty"`
	}{
		TransactionId:   x.GetTransactionId(),
		Force:           x.GetForce(),
		AtEffectiveDate: x.GetAtEffectiveDate(),
		Metadata:        commonpb.MetadataToAnyMap(x.GetMetadata()),
		Receipt:         x.GetReceipt(),
		ExpandVolumes:   x.GetExpandVolumes(),
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
		TargetType string         `json:"targetType"`
		TargetID   json.RawValue  `json:"targetId"`
		Metadata   map[string]any `json:"metadata"`
	}

	var raw rawReq
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	ms, err := commonpb.MetadataFromAnyMap(raw.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	target, err := commonpb.ParseTarget(raw.TargetType, raw.TargetID)
	if err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	return &commonpb.SaveMetadataCommand{
		Target:   target,
		Metadata: ms,
	}, nil
}

// unmarshalRevertTransactionPayload unmarshals JSON into RevertTransactionPayload.
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

	ms, err := commonpb.MetadataFromAnyMap(raw.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	if raw.ID == 0 {
		return nil, errors.New("revert payload requires id")
	}

	return &RevertTransactionPayload{
		TransactionId:   raw.ID,
		Force:           raw.Force,
		AtEffectiveDate: raw.AtEffectiveDate,
		Metadata:        ms,
	}, nil
}

// unmarshalDeleteMetadataCommand unmarshals JSON into DeleteMetadataCommand.
func unmarshalDeleteMetadataCommand(data json.RawValue) (*commonpb.DeleteMetadataCommand, error) {
	type rawReq struct {
		TargetType string        `json:"targetType"`
		TargetID   json.RawValue `json:"targetId"`
		Key        string        `json:"key"`
	}

	var raw rawReq

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}

	target, err := commonpb.ParseTarget(raw.TargetType, raw.TargetID)
	if err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	return &commonpb.DeleteMetadataCommand{
		Target: target,
		Key:    raw.Key,
	}, nil
}
