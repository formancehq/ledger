package ledgerpb

import (
	"encoding/json"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

// LogType constants for log payload types
const (
	SetMetadataLogType         LogType = 0 // "SET_METADATA"
	NewTransactionLogType      LogType = 1 // "NEW_TRANSACTION"
	RevertedTransactionLogType LogType = 2 // "REVERTED_TRANSACTION"
	DeleteMetadataLogType      LogType = 3 // "DELETE_METADATA"
)

// HydrateLog deserializes a log payload from JSON based on the log type
func HydrateLog(logType LogType, data []byte) (LogPayloadInterface, error) {
	var payload proto.Message
	switch logType {
	case NewTransactionLogType:
		payload = &CreatedTransaction{}
	case SetMetadataLogType:
		payload = &SavedMetadata{}
	case DeleteMetadataLogType:
		payload = &DeletedMetadata{}
	case RevertedTransactionLogType:
		payload = &RevertedTransaction{}
	default:
		return nil, fmt.Errorf("unknown log type: %d", logType)
	}
	err := json.Unmarshal(data, payload)
	if err != nil {
		return nil, err
	}

	return reflect.ValueOf(payload).Interface().(LogPayloadInterface), nil
}

// LogPayloadToProtobuf converts a LogPayloadInterface to protobuf LogPayload
func LogPayloadToProtobuf(payload LogPayloadInterface) (*LogPayload, error) {
	switch p := payload.(type) {
	case *CreatedTransaction:
		return &LogPayload{
			Payload: &LogPayload_CreatedTransaction{
				CreatedTransaction: p,
			},
		}, nil
	case *RevertedTransaction:
		return &LogPayload{
			Payload: &LogPayload_RevertedTransaction{
				RevertedTransaction: p,
			},
		}, nil
	case *SavedMetadata:
		return &LogPayload{
			Payload: &LogPayload_SavedMetadata{
				SavedMetadata: p,
			},
		}, nil
	case *DeletedMetadata:
		return &LogPayload{
			Payload: &LogPayload_DeletedMetadata{
				DeletedMetadata: p,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown log payload type: %T", payload)
	}
}
