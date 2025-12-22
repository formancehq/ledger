package ledgerpb

import (
	"database/sql/driver"
	"encoding/json"
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
	}

	panic("invalid log type")
}


// GetLogType extracts the log type from a LogPayload
func GetLogType(payload *LogPayload) LogType {
	if payload == nil {
		return 0
	}
	switch payload.Payload.(type) {
	case *LogPayload_CreatedTransaction:
		return NewTransactionLogType
	case *LogPayload_RevertedTransaction:
		return RevertedTransactionLogType
	case *LogPayload_SavedMetadata:
		return SetMetadataLogType
	case *LogPayload_DeletedMetadata:
		return DeleteMetadataLogType
	default:
		return 0
	}
}

// GetLogTypeFromLog extracts the log type from a Log
func GetLogTypeFromLog(log *Log) LogType {
	if log == nil || log.Data == nil {
		return 0
	}
	return GetLogType(log.Data)
}

