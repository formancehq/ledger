package commonpb

import (
	"database/sql/driver"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

type LogType int16

func (lt LogType) Value() (driver.Value, error) {
	return lt.String(), nil
}

func (lt *LogType) Scan(src any) error {
	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("LogType.Scan: expected string, got %T", src)
	}

	v, err := LogTypeFromString(s)
	if err != nil {
		return err
	}

	*lt = v

	return nil
}

func (lt LogType) MarshalJSON() ([]byte, error) {
	return json.Marshal(lt.String())
}

func (lt *LogType) UnmarshalJSON(data []byte) error {
	var s string

	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	v, err := LogTypeFromString(s)
	if err != nil {
		return err
	}

	*lt = v

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
	}

	return ""
}

func LogTypeFromString(logType string) (LogType, error) {
	switch logType {
	case "SET_METADATA":
		return SetMetadataLogType, nil
	case "NEW_TRANSACTION":
		return NewTransactionLogType, nil
	case "REVERTED_TRANSACTION":
		return RevertedTransactionLogType, nil
	case "DELETE_METADATA":
		return DeleteMetadataLogType, nil
	case "SET_METADATA_FIELD_TYPE":
		return SetMetadataFieldTypeLogType, nil
	case "REMOVED_METADATA_FIELD_TYPE":
		return RemovedMetadataFieldTypeLogType, nil
	}

	return 0, fmt.Errorf("invalid log type: %q", logType)
}

// GetLogType extracts the log type from a LedgerLogPayload.
func GetLogType(payload *LedgerLogPayload) LogType {
	if payload == nil {
		return 0
	}

	switch payload.GetPayload().(type) {
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
	default:
		return 0
	}
}

// GetLogTypeFromLog extracts the log type from a LedgerLog.
func GetLogTypeFromLog(log *LedgerLog) LogType {
	if log == nil || log.GetData() == nil {
		return 0
	}

	return GetLogType(log.GetData())
}
