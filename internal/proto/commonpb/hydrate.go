package commonpb

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
)

// LogType constants for log payload types.
const (
	SetMetadataLogType                LogType = 0 // "SET_METADATA"
	NewTransactionLogType             LogType = 1 // "NEW_TRANSACTION"
	RevertedTransactionLogType        LogType = 2 // "REVERTED_TRANSACTION"
	DeleteMetadataLogType             LogType = 3 // "DELETE_METADATA"
	SetMetadataFieldTypeLogType       LogType = 4 // "SET_METADATA_FIELD_TYPE"
	RemovedMetadataFieldTypeLogType   LogType = 5 // "REMOVED_METADATA_FIELD_TYPE"
	ConvertMetadataBatchLogType       LogType = 6 // "CONVERT_METADATA_BATCH"
	MetadataConversionCompleteLogType LogType = 7 // "METADATA_CONVERSION_COMPLETE"
)

// HydrateLog deserializes a log payload from JSON based on the log type.
func HydrateLog(logType LogType, data []byte) (proto.Message, error) {
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
	case SetMetadataFieldTypeLogType:
		payload = &SetMetadataFieldTypeLog{}
	case RemovedMetadataFieldTypeLogType:
		payload = &RemovedMetadataFieldTypeLog{}
	case ConvertMetadataBatchLogType:
		payload = &ConvertMetadataBatchLog{}
	case MetadataConversionCompleteLogType:
		payload = &MetadataConversionCompleteLog{}
	default:
		return nil, fmt.Errorf("unknown log type: %d", logType)
	}

	err := json.Unmarshal(data, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
