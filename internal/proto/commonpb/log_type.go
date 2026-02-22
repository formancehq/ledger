package commonpb

import (
	"database/sql/driver"

	"github.com/formancehq/ledger-v3-poc/internal/compat/json"
)

type LogType int16

func (lt LogType) Value() (driver.Value, error) {
	return lt.String(), nil
}

func (lt *LogType) Scan(src interface{}) error {
	*lt = LogTypeFromString(src.(string))
	return nil
}

func (lt LogType) MarshalJSON() ([]byte, error) {
	return json.Marshal(lt.String())
}

func (lt *LogType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	*lt = LogTypeFromString(s)

	return nil
}

func (lt LogType) String() string {
	switch lt {
	case SetMetadataLogType:
		return "SET_METADATA"
	case NewTransactionLogType:
		return "NEW_TRANSACTION"
	case RevertedTransactionLogType:
		return "REVERTED_TRANSACTION"
	case DeleteMetadataLogType:
		return "DELETE_METADATA"
	case SetMetadataFieldTypeLogType:
		return "SET_METADATA_FIELD_TYPE"
	case RemovedMetadataFieldTypeLogType:
		return "REMOVED_METADATA_FIELD_TYPE"
	case ConvertMetadataBatchLogType:
		return "CONVERT_METADATA_BATCH"
	case MetadataConversionCompleteLogType:
		return "METADATA_CONVERSION_COMPLETE"
	}

	return ""
}

func LogTypeFromString(logType string) LogType {
	switch logType {
	case "SET_METADATA":
		return SetMetadataLogType
	case "NEW_TRANSACTION":
		return NewTransactionLogType
	case "REVERTED_TRANSACTION":
		return RevertedTransactionLogType
	case "DELETE_METADATA":
		return DeleteMetadataLogType
	case "SET_METADATA_FIELD_TYPE":
		return SetMetadataFieldTypeLogType
	case "REMOVED_METADATA_FIELD_TYPE":
		return RemovedMetadataFieldTypeLogType
	case "CONVERT_METADATA_BATCH":
		return ConvertMetadataBatchLogType
	case "METADATA_CONVERSION_COMPLETE":
		return MetadataConversionCompleteLogType
	}

	panic("invalid log type")
}

// GetLogType extracts the log type from a LedgerLogPayload
func GetLogType(payload *LedgerLogPayload) LogType {
	if payload == nil {
		return 0
	}
	switch payload.Payload.(type) {
	case *LedgerLogPayload_CreatedTransaction:
		return NewTransactionLogType
	case *LedgerLogPayload_RevertedTransaction:
		return RevertedTransactionLogType
	case *LedgerLogPayload_SavedMetadata:
		return SetMetadataLogType
	case *LedgerLogPayload_DeletedMetadata:
		return DeleteMetadataLogType
	case *LedgerLogPayload_SetMetadataFieldType:
		return SetMetadataFieldTypeLogType
	case *LedgerLogPayload_RemovedMetadataFieldType:
		return RemovedMetadataFieldTypeLogType
	case *LedgerLogPayload_ConvertMetadataBatch:
		return ConvertMetadataBatchLogType
	case *LedgerLogPayload_MetadataConversionComplete:
		return MetadataConversionCompleteLogType
	default:
		return 0
	}
}

// GetLogTypeFromLog extracts the log type from a LedgerLog
func GetLogTypeFromLog(log *LedgerLog) LogType {
	if log == nil || log.Data == nil {
		return 0
	}
	return GetLogType(log.Data)
}
