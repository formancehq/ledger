package commonpb

import (
	"database/sql/driver"
	"fmt"

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
	case OrderSkippedLogType:
		return "ORDER_SKIPPED"
	case FillGapLogType:
		return "FILL_GAP"
	case CreateIndexLogType:
		return "CREATE_INDEX"
	case DropIndexLogType:
		return "DROP_INDEX"
	case AddedAccountTypeLogType:
		return "ADDED_ACCOUNT_TYPE"
	case RemovedAccountTypeLogType:
		return "REMOVED_ACCOUNT_TYPE"
	case UpdatedDefaultEnforcementModeLogType:
		return "UPDATED_DEFAULT_ENFORCEMENT_MODE"
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
	case "ORDER_SKIPPED":
		return OrderSkippedLogType, nil
	case "FILL_GAP":
		return FillGapLogType, nil
	case "CREATE_INDEX":
		return CreateIndexLogType, nil
	case "DROP_INDEX":
		return DropIndexLogType, nil
	case "ADDED_ACCOUNT_TYPE":
		return AddedAccountTypeLogType, nil
	case "REMOVED_ACCOUNT_TYPE":
		return RemovedAccountTypeLogType, nil
	case "UPDATED_DEFAULT_ENFORCEMENT_MODE":
		return UpdatedDefaultEnforcementModeLogType, nil
	}

	return 0, fmt.Errorf("invalid log type: %q", logType)
}

// GetLogType extracts the log type from a LedgerLogPayload.
func GetLogType(payload *LedgerLogPayload) LogType {
	if payload == nil {
		return -1
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
	case *LedgerLogPayload_OrderSkipped:
		return OrderSkippedLogType
	case *LedgerLogPayload_FillGap:
		return FillGapLogType
	case *LedgerLogPayload_CreateIndex:
		return CreateIndexLogType
	case *LedgerLogPayload_DropIndex:
		return DropIndexLogType
	case *LedgerLogPayload_AddedAccountType:
		return AddedAccountTypeLogType
	case *LedgerLogPayload_RemovedAccountType:
		return RemovedAccountTypeLogType
	case *LedgerLogPayload_UpdatedDefaultEnforcementMode:
		return UpdatedDefaultEnforcementModeLogType
	default:
		return -1
	}
}

// GetLogTypeFromLog extracts the log type from a LedgerLog.
func GetLogTypeFromLog(log *LedgerLog) LogType {
	if log == nil || log.GetData() == nil {
		return -1
	}

	return GetLogType(log.GetData())
}
