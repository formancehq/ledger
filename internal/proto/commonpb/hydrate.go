package commonpb

import (
	"bytes"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// LogType constants for log payload types.
const (
	SetMetadataLogType                   LogType = 0  // "SET_METADATA"
	NewTransactionLogType                LogType = 1  // "NEW_TRANSACTION"
	RevertedTransactionLogType           LogType = 2  // "REVERTED_TRANSACTION"
	DeleteMetadataLogType                LogType = 3  // "DELETE_METADATA"
	SetMetadataFieldTypeLogType          LogType = 4  // "SET_METADATA_FIELD_TYPE"
	RemovedMetadataFieldTypeLogType      LogType = 5  // "REMOVED_METADATA_FIELD_TYPE"
	OrderSkippedLogType                  LogType = 6  // "ORDER_SKIPPED"
	FillGapLogType                       LogType = 7  // "FILL_GAP"
	CreateIndexLogType                   LogType = 8  // "CREATE_INDEX"
	DropIndexLogType                     LogType = 9  // "DROP_INDEX"
	AddedAccountTypeLogType              LogType = 10 // "ADDED_ACCOUNT_TYPE"
	RemovedAccountTypeLogType            LogType = 11 // "REMOVED_ACCOUNT_TYPE"
	UpdatedDefaultEnforcementModeLogType LogType = 12 // "UPDATED_DEFAULT_ENFORCEMENT_MODE"
)

// HydrateLog decodes the direct data object emitted by LedgerLog.MarshalJSON.
// Each payload variant has a distinct log type, following the v2 JSON layout.
func HydrateLog(logType LogType, data []byte) (proto.Message, error) {
	payload, err := hydrateLedgerLogPayload(logType, data)
	if err != nil {
		return nil, err
	}

	return payload.jsonMessage()
}

func hydrateLedgerLogPayload(logType LogType, data []byte) (*LedgerLogPayload, error) {
	payload := &LedgerLogPayload{}
	switch logType {
	case NewTransactionLogType:
		payload.Payload = &LedgerLogPayload_CreatedTransaction{CreatedTransaction: &CreatedTransaction{}}
	case RevertedTransactionLogType:
		payload.Payload = &LedgerLogPayload_RevertedTransaction{RevertedTransaction: &RevertedTransaction{}}
	case SetMetadataLogType:
		payload.Payload = &LedgerLogPayload_SavedMetadata{SavedMetadata: &SavedMetadata{}}
	case DeleteMetadataLogType:
		payload.Payload = &LedgerLogPayload_DeletedMetadata{DeletedMetadata: &DeletedMetadata{}}
	case SetMetadataFieldTypeLogType:
		payload.Payload = &LedgerLogPayload_SetMetadataFieldType{SetMetadataFieldType: &SetMetadataFieldTypeLog{}}
	case RemovedMetadataFieldTypeLogType:
		payload.Payload = &LedgerLogPayload_RemovedMetadataFieldType{RemovedMetadataFieldType: &RemovedMetadataFieldTypeLog{}}
	case FillGapLogType:
		payload.Payload = &LedgerLogPayload_FillGap{FillGap: &FilledGapLog{}}
	case CreateIndexLogType:
		payload.Payload = &LedgerLogPayload_CreateIndex{CreateIndex: &CreatedIndexLog{}}
	case DropIndexLogType:
		payload.Payload = &LedgerLogPayload_DropIndex{DropIndex: &DroppedIndexLog{}}
	case AddedAccountTypeLogType:
		payload.Payload = &LedgerLogPayload_AddedAccountType{AddedAccountType: &AddedAccountTypeLog{}}
	case RemovedAccountTypeLogType:
		payload.Payload = &LedgerLogPayload_RemovedAccountType{RemovedAccountType: &RemovedAccountTypeLog{}}
	case UpdatedDefaultEnforcementModeLogType:
		payload.Payload = &LedgerLogPayload_UpdatedDefaultEnforcementMode{UpdatedDefaultEnforcementMode: &UpdatedDefaultEnforcementModeLog{}}
	case OrderSkippedLogType:
		payload.Payload = &LedgerLogPayload_OrderSkipped{OrderSkipped: &OrderSkippedLog{}}
	default:
		return nil, fmt.Errorf("unknown log type: %d", logType)
	}

	var fields map[string]json.RawValue
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, errors.New("log data must be an object")
	}
	if logType == OrderSkippedLogType {
		if reason, ok := fields["reason"]; !ok || bytes.Equal(bytes.TrimSpace(reason), []byte("null")) {
			return nil, errors.New("ORDER_SKIPPED data must contain a reason")
		}
	}
	message, err := payload.jsonMessage()
	if err != nil {
		return nil, err
	}
	if custom, ok := message.(interface{ UnmarshalJSON([]byte) error }); ok {
		err = custom.UnmarshalJSON(data)
	} else {
		err = protojson.Unmarshal(data, message)
	}
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// logMetadataJSON decodes response metadata without request-side null deletion
// or float64 rounding. JSON does not carry the original signed/datetime branch
// or NullValue.original; hydration preserves the emitted values, not that
// protobuf-only provenance.
type logMetadataJSON map[string]*MetadataValue

func (m *logMetadataJSON) UnmarshalJSON(data []byte) error {
	var values map[string]json.RawValue
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	if values == nil {
		*m = nil

		return nil
	}
	result := make(logMetadataJSON, len(values))
	for key, raw := range values {
		value := bytes.TrimSpace(raw)
		switch value[0] {
		case 'n':
			result[key] = NewNullValue("")
		case '"':
			var v string
			if err := json.Unmarshal(value, &v); err != nil {
				return err
			}
			result[key] = NewStringValue(v)
		case 't', 'f':
			var v bool
			if err := json.Unmarshal(value, &v); err != nil {
				return err
			}
			result[key] = NewBoolValue(v)
		case '-':
			var v int64
			if err := json.Unmarshal(value, &v); err != nil {
				return fmt.Errorf("metadata key %q: %w", key, err)
			}
			result[key] = NewIntValue(v)
		default:
			var v uint64
			if err := json.Unmarshal(value, &v); err != nil {
				return fmt.Errorf("metadata key %q: %w", key, err)
			}
			result[key] = NewUintValue(v)
		}
	}
	*m = result

	return nil
}
