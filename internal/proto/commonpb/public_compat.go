package commonpb

import ledgerpb "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

var (
	NewStringValue                           = ledgerpb.NewStringValue
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
