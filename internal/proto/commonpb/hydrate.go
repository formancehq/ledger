package commonpb

import (
	"fmt"
	"reflect"

	"github.com/formancehq/ledger-v3-poc/internal/json"
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

