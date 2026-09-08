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
	SetMetadataLogType              LogType = 0 // "SET_METADATA"
	NewTransactionLogType           LogType = 1 // "NEW_TRANSACTION"
	RevertedTransactionLogType      LogType = 2 // "REVERTED_TRANSACTION"
	DeleteMetadataLogType           LogType = 3 // "DELETE_METADATA"
	SetMetadataFieldTypeLogType     LogType = 4 // "SET_METADATA_FIELD_TYPE"
	RemovedMetadataFieldTypeLogType LogType = 5 // "REMOVED_METADATA_FIELD_TYPE"
	OrderSkippedLogType             LogType = 6 // "ORDER_SKIPPED"
)

// HydrateLog decodes the data emitted by LedgerLog.MarshalJSON. All variants
// use a oneof envelope except ORDER_SKIPPED. The envelope also identifies the
// variants whose published discriminator is the default SET_METADATA.
func HydrateLog(logType LogType, data []byte) (proto.Message, error) {
	payload, err := hydrateLedgerLogPayload(logType, data)
	if err != nil {
		return nil, err
	}
	message := payload.ProtoReflect()
	field := message.WhichOneof(message.Descriptor().Oneofs().Get(0))

	return message.Get(field).Message().Interface(), nil
}

func hydrateLedgerLogPayload(logType LogType, data []byte) (*LedgerLogPayload, error) {
	if logType.String() == "" {
		return nil, fmt.Errorf("unknown log type: %d", logType)
	}
	var envelope map[string]json.RawValue
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	if logType == OrderSkippedLogType {
		if reason, ok := envelope["reason"]; !ok || bytes.Equal(bytes.TrimSpace(reason), []byte("null")) {
			return nil, errors.New("ORDER_SKIPPED data must contain a reason")
		}
		skipped := &OrderSkippedLog{}
		if err := json.Unmarshal(data, skipped); err != nil {
			return nil, err
		}

		return &LedgerLogPayload{Payload: &LedgerLogPayload_OrderSkipped{OrderSkipped: skipped}}, nil
	}
	if len(envelope) != 1 {
		return nil, errors.New("log data must contain exactly one payload")
	}
	payload := &LedgerLogPayload{}
	for key, innerData := range envelope {
		if bytes.Equal(bytes.TrimSpace(innerData), []byte("null")) {
			return nil, fmt.Errorf("log payload %q is null", key)
		}
		var inner proto.Message
		switch key {
		case "createdTransaction":
			value := &CreatedTransaction{}
			payload.Payload = &LedgerLogPayload_CreatedTransaction{CreatedTransaction: value}
			inner = value
		case "revertedTransaction":
			value := &RevertedTransaction{}
			payload.Payload = &LedgerLogPayload_RevertedTransaction{RevertedTransaction: value}
			inner = value
		case "savedMetadata":
			value := &SavedMetadata{}
			payload.Payload = &LedgerLogPayload_SavedMetadata{SavedMetadata: value}
			inner = value
		case "deletedMetadata":
			value := &DeletedMetadata{}
			payload.Payload = &LedgerLogPayload_DeletedMetadata{DeletedMetadata: value}
			inner = value
		default:
			// The remaining envelopes are emitted by protojson, including enum
			// names, quoted integers and nested protobuf oneofs.
			if err := protojson.Unmarshal(data, payload); err != nil {
				return nil, err
			}
		}
		if inner != nil {
			if err := json.Unmarshal(innerData, inner); err != nil {
				return nil, err
			}
		}
	}
	if payload.GetPayload() == nil {
		return nil, errors.New("missing log payload")
	}
	if GetLogType(payload) != logType {
		return nil, fmt.Errorf("log type %s does not match payload", logType)
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
