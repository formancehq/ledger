package grpc

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/pkg/client/v3/internal/json"
)

// errorReasonPrefix mirrors domain.errorReasonPrefix without taking on a
// domain import (this proto package sits below domain). Kept in lockstep
// with internal/domain/reason.go: every ErrorReason enum constant
// is errorReasonPrefix + the short, client-facing Reason() string the
// gRPC/HTTP error surface uses.
const errorReasonPrefix = "ERROR_REASON_"

// MarshalJSON implements json.Marshaler for GetTransactionResponse.
func (x *GetTransactionResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Transaction *Transaction `json:"transaction,omitempty"`
	}{
		Transaction: x.GetTransaction(),
	})
}

// MarshalJSON implements json.Marshaler for CreateTransactionPayload.
func (x *CreateTransactionPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		AccountMetadata map[string]map[string]any `json:"accountMetadata,omitempty"`
		Metadata        map[string]any            `json:"metadata,omitempty"`
		Timestamp       *Timestamp                `json:"timestamp,omitempty"`
		Reference       string                    `json:"reference,omitempty"`
		Postings        []*Posting                `json:"postings,omitempty"`
		Script          *Script                   `json:"script,omitempty"`
		ScriptReference *ScriptReference          `json:"scriptReference,omitempty"`
		Force           bool                      `json:"force,omitempty"`
	}{
		AccountMetadata: AccountMetadataToAnyMap(x.GetAccountMetadata()),
		Metadata:        MetadataToAnyMap(x.GetMetadata()),
		Timestamp:       x.GetTimestamp(),
		Reference:       x.GetReference(),
		Postings:        x.GetPostings(),
		Script:          x.GetScript(),
		ScriptReference: x.GetScriptReference(),
		Force:           x.GetForce(),
	})
}

// UnmarshalJSON implements json.Unmarshaler for CreateTransactionPayload.
//
// The default protoc-gen-go struct tags are snake_case, so a plain
// encoding/json decode would silently drop the multi-word camelCase keys the
// REST contract advertises (scriptReference, accountMetadata, …) and produce a
// zero-posting transaction (#452). We mirror MarshalJSON's shape here, then
// rebuild the protobuf struct field-by-field. Unknown JSON keys are tolerated
// to preserve the lenient behavior of the previous decoder.
func (x *CreateTransactionPayload) UnmarshalJSON(data []byte) error {
	var aux struct {
		AccountMetadata map[string]map[string]any `json:"accountMetadata"`
		Metadata        map[string]any            `json:"metadata"`
		Timestamp       *Timestamp                `json:"timestamp"`
		Reference       string                    `json:"reference"`
		Postings        []*Posting                `json:"postings"`
		Script          *Script                   `json:"script"`
		ScriptReference *ScriptReference          `json:"scriptReference"`
		Force           bool                      `json:"force"`
	}

	if err := json.UnmarshalUseNumber(data, &aux); err != nil {
		return err
	}

	metadata, err := MetadataFromAnyMap(aux.Metadata)
	if err != nil {
		return fmt.Errorf("invalid metadata: %w", err)
	}

	var accountMetadata map[string]*MetadataMap
	if len(aux.AccountMetadata) > 0 {
		accountMetadata = make(map[string]*MetadataMap, len(aux.AccountMetadata))

		for account, values := range aux.AccountMetadata {
			mv, err := MetadataFromAnyMap(values)
			if err != nil {
				return fmt.Errorf("invalid account metadata for %q: %w", account, err)
			}

			accountMetadata[account] = &MetadataMap{Values: mv}
		}
	}

	x.Postings = aux.Postings
	x.Script = aux.Script
	x.Timestamp = aux.Timestamp
	x.Reference = aux.Reference
	x.Metadata = metadata
	x.AccountMetadata = accountMetadata
	x.Force = aux.Force
	x.ScriptReference = aux.ScriptReference

	return nil
}

// errorReasonsFromStrings parses the short-form ErrorReason list a REST
// caller submits (e.g. "TRANSACTION_REFERENCE_CONFLICT" — the same
// identifier the gRPC ErrorInfo.reason and REST error responses use).
// Re-prepends the "ERROR_REASON_" prefix to match the generated enum
// constant before looking it up. Unknown names fail loudly so a typo in
// `skippableReasons` is rejected at admission with a clear 400 rather
// than silently dropped.
func errorReasonsFromStrings(in []string) ([]ErrorReason, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make([]ErrorReason, len(in))

	for i, name := range in {
		code, ok := ErrorReason_value[errorReasonPrefix+name]
		if !ok {
			return nil, fmt.Errorf("unknown ErrorReason %q at index %d", name, i)
		}

		out[i] = ErrorReason(code)
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
	}{
		TransactionId:   x.GetTransactionId(),
		Force:           x.GetForce(),
		AtEffectiveDate: x.GetAtEffectiveDate(),
		Metadata:        MetadataToAnyMap(x.GetMetadata()),
	})
}

// Ledger action type constants.
const (
	LedgerActionTypeCreateTransaction = "CREATE_TRANSACTION"
	LedgerActionTypeAddMetadata       = "ADD_METADATA"
	LedgerActionTypeRevertTransaction = "REVERT_TRANSACTION"
	LedgerActionTypeDeleteMetadata    = "DELETE_METADATA"
)

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
func unmarshalSaveMetadataCommand(data json.RawValue) (*SaveMetadataCommand, error) {
	type rawReq struct {
		TargetType string         `json:"targetType"`
		TargetID   json.RawValue  `json:"targetId"`
		Metadata   map[string]any `json:"metadata"`
	}

	var raw rawReq
	if err := json.UnmarshalUseNumber(data, &raw); err != nil {
		return nil, err
	}

	ms, err := MetadataFromAnyMap(raw.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	target, err := ParseTarget(raw.TargetType, raw.TargetID)
	if err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	return &SaveMetadataCommand{
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
	if err := json.UnmarshalUseNumber(data, &raw); err != nil {
		return nil, err
	}

	ms, err := MetadataFromAnyMap(raw.Metadata)
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
func unmarshalDeleteMetadataCommand(data json.RawValue) (*DeleteMetadataCommand, error) {
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

	target, err := ParseTarget(raw.TargetType, raw.TargetID)
	if err != nil {
		return nil, fmt.Errorf("invalid target: %w", err)
	}

	return &DeleteMetadataCommand{
		Target: target,
		Key:    raw.Key,
	}, nil
}
