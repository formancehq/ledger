package commonpb

import ledgerpb "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

type LedgerLogPayload_AccountMetadataUpdated = ledgerpb.LedgerLogPayload_AccountMetadataUpdated
type AccountMetadataUpdated = ledgerpb.AccountMetadataUpdated
type CapabilityState = ledgerpb.CapabilityState
type MirrorAssetMapping = ledgerpb.MirrorAssetMapping
type LedgerLogCategory = ledgerpb.LedgerLogCategory
type NotFoundError = ledgerpb.NotFoundError
type SegmentTypeJSON = ledgerpb.SegmentTypeJSON

const (
	LedgerLogCategory_LEDGER_LOG_CATEGORY_UNSPECIFIED = ledgerpb.LedgerLogCategory_LEDGER_LOG_CATEGORY_UNSPECIFIED
	LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY     = ledgerpb.LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY
	AuthScope_AUTH_SCOPE_EXECUTE_NUMSCRIPT            = ledgerpb.AuthScope_AUTH_SCOPE_EXECUTE_NUMSCRIPT
)

func LessByAssetColor[T ledgerpb.AssetColored](a, b T) bool { return ledgerpb.LessByAssetColor(a, b) }
func MetadataFromGoMap[M ~map[string]string](m M) map[string]*ledgerpb.MetadataValue {
	return ledgerpb.MetadataFromGoMap(m)
}

var (
	NewStringValue                           = ledgerpb.NewStringValue
	SegmentTypeFromJSON                      = ledgerpb.SegmentTypeFromJSON
	SegmentTypeToJSON                        = ledgerpb.SegmentTypeToJSON
	MetadataFromAnyMap                       = ledgerpb.MetadataFromAnyMap
	ParsePersistence                         = ledgerpb.ParsePersistence
	PersistenceToString                      = ledgerpb.PersistenceToString
	MetadataToAnyMap                         = ledgerpb.MetadataToAnyMap
	ParseBigUint                             = ledgerpb.ParseBigUint
	AccountMetadataToAnyMap                  = ledgerpb.AccountMetadataToAnyMap
	MetadataValueFromAny                     = ledgerpb.MetadataValueFromAny
	SchemaFieldForTarget                     = ledgerpb.SchemaFieldForTarget
	ErrNoLeader                              = ledgerpb.ErrNoLeader
	NewNotFoundError                         = ledgerpb.NewNotFoundError
	ConvertMetadataValue                     = ledgerpb.ConvertMetadataValue
	CoerceToDeclaredType                     = ledgerpb.CoerceToDeclaredType
	CoerceDatetimeMicros                     = ledgerpb.CoerceDatetimeMicros
	LedgerLogCategoryOf                      = ledgerpb.LedgerLogCategoryOf
	IsLedgerHistoryPayload                   = ledgerpb.IsLedgerHistoryPayload
	TypeMatches                              = ledgerpb.TypeMatches
	MetadataTypeToString                     = ledgerpb.MetadataTypeToString
	ParseTargetType                          = ledgerpb.ParseTargetType
	ParseMetadataType                        = ledgerpb.ParseMetadataType
	NewBigUint                               = ledgerpb.NewBigUint
	NewBigUintFromUint256                    = ledgerpb.NewBigUintFromUint256
	NewUint256                               = ledgerpb.NewUint256
	MetadataValueToString                    = ledgerpb.MetadataValueToString
	ConditionKindOf                          = ledgerpb.ConditionKindOf
	ConditionValidForTarget                  = ledgerpb.ConditionValidForTarget
	TargetHumanName                          = ledgerpb.TargetHumanName
	IsSignedType                             = ledgerpb.IsSignedType
	IsDatetimeType                           = ledgerpb.IsDatetimeType
	IsUnsignedType                           = ledgerpb.IsUnsignedType
	NewIntValue                              = ledgerpb.NewIntValue
	NewUintValue                             = ledgerpb.NewUintValue
	NewBoolValue                             = ledgerpb.NewBoolValue
	NewNullValue                             = ledgerpb.NewNullValue
	NewDatetimeValue                         = ledgerpb.NewDatetimeValue
	NewPosting                               = ledgerpb.NewPosting
	NewColoredPosting                        = ledgerpb.NewColoredPosting
	NewTransaction                           = ledgerpb.NewTransaction
	NewLedgerLog                             = ledgerpb.NewLedgerLog
	NewTimestamp                             = ledgerpb.NewTimestamp
	NewUint256FromUint64                     = ledgerpb.NewUint256FromUint64
	MustBigUintFromDecimal                   = ledgerpb.MustBigUintFromDecimal
	MustSignedBigIntFromDecimal              = ledgerpb.MustSignedBigIntFromDecimal
	BigIntegersAsNumbers                     = ledgerpb.BigIntegersAsNumbers
	BigIntegersAsStrings                     = ledgerpb.BigIntegersAsStrings
	AllRPCAuthPolicies                       = ledgerpb.AllRPCAuthPolicies
	RPCAuthPolicyForMethod                   = ledgerpb.RPCAuthPolicyForMethod
	MustFailureDetails                       = ledgerpb.MustFailureDetails
	LogTypeFromString                        = ledgerpb.LogTypeFromString
	GetLogType                               = ledgerpb.GetLogType
	NewTransactionListReader                 = ledgerpb.NewTransactionListReader
	NewTouchedVolumeListReader               = ledgerpb.NewTouchedVolumeListReader
	NewSetMetadataFieldTypeCommandListReader = ledgerpb.NewSetMetadataFieldTypeCommandListReader
)

const (
	SetMetadataLogType                   = ledgerpb.SetMetadataLogType
	NewTransactionLogType                = ledgerpb.NewTransactionLogType
	RevertedTransactionLogType           = ledgerpb.RevertedTransactionLogType
	DeleteMetadataLogType                = ledgerpb.DeleteMetadataLogType
	SetMetadataFieldTypeLogType          = ledgerpb.SetMetadataFieldTypeLogType
	RemovedMetadataFieldTypeLogType      = ledgerpb.RemovedMetadataFieldTypeLogType
	OrderSkippedLogType                  = ledgerpb.OrderSkippedLogType
	FillGapLogType                       = ledgerpb.FillGapLogType
	CreateIndexLogType                   = ledgerpb.CreateIndexLogType
	DropIndexLogType                     = ledgerpb.DropIndexLogType
	AddedAccountTypeLogType              = ledgerpb.AddedAccountTypeLogType
	RemovedAccountTypeLogType            = ledgerpb.RemovedAccountTypeLogType
	UpdatedDefaultEnforcementModeLogType = ledgerpb.UpdatedDefaultEnforcementModeLogType
)

type LogType = ledgerpb.LogType

func (x *IdempotencyFailure) GetReason() ledgerpb.ErrorReason {
	return ledgerpb.FailureDetailsReason(x.GetDetails())
}

func (x *IdempotencyFailure) GetCode() string {
	return ledgerpb.FailureDetailsCode(x.GetDetails())
}

func (x *IdempotencyFailure) GetFacts() map[string]string {
	return ledgerpb.FailureDetailsFacts(x.GetDetails())
}

type MetadataValue_TypeReaderCase = ledgerpb.MetadataValue_TypeReaderCase

const (
	EventType_ACCOUNT_METADATA_UPDATED       = ledgerpb.EventType_ACCOUNT_METADATA_UPDATED
	MetadataValue_TypeReaderCase_Unset       = ledgerpb.MetadataValue_TypeReaderCase_Unset
	MetadataValue_TypeReaderCase_StringValue = ledgerpb.MetadataValue_TypeReaderCase_StringValue
	MetadataValue_TypeReaderCase_IntValue    = ledgerpb.MetadataValue_TypeReaderCase_IntValue
	MetadataValue_TypeReaderCase_BoolValue   = ledgerpb.MetadataValue_TypeReaderCase_BoolValue
	MetadataValue_TypeReaderCase_NullValue   = ledgerpb.MetadataValue_TypeReaderCase_NullValue
	AccountMetadataUpdatedLogType            = ledgerpb.AccountMetadataUpdatedLogType
)

func MetadataMapFromGoMap[M ~map[string]string](m M) *ledgerpb.MetadataMap {
	return ledgerpb.MetadataMapFromGoMap(m)
}

var (
	NewBigUintFromUint64         = ledgerpb.NewBigUintFromUint64
	MetadataToGoMap              = ledgerpb.MetadataToGoMap
	MetadataMapToGoMap           = ledgerpb.MetadataMapToGoMap
	MetadataValueToAny           = ledgerpb.MetadataValueToAny
	DecodeQueryFilterJSON        = ledgerpb.DecodeQueryFilterJSON
	FailureDetailsReason         = ledgerpb.FailureDetailsReason
	NewFailureDetails            = ledgerpb.NewFailureDetails
	FailureDetailsCode           = ledgerpb.FailureDetailsCode
	ValidateRequiredFailureFacts = ledgerpb.ValidateRequiredFailureFacts
	FailureDetailsFacts          = ledgerpb.FailureDetailsFacts
)

type SegmentType_ConstraintReaderCase = ledgerpb.SegmentType_ConstraintReaderCase

const (
	SegmentType_ConstraintReaderCase_Regex  = ledgerpb.SegmentType_ConstraintReaderCase_Regex
	SegmentType_ConstraintReaderCase_Uuid   = ledgerpb.SegmentType_ConstraintReaderCase_Uuid
	SegmentType_ConstraintReaderCase_Uint64 = ledgerpb.SegmentType_ConstraintReaderCase_Uint64
	SegmentType_ConstraintReaderCase_Bytes  = ledgerpb.SegmentType_ConstraintReaderCase_Bytes
)
