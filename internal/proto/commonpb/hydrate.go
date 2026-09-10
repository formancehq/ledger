package commonpb

import (
	"fmt"

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
	case OrderSkippedLogType:
		payload = &OrderSkippedLog{}
	default:
		return nil, fmt.Errorf("unknown log type: %d", logType)
	}

	err := json.Unmarshal(data, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
