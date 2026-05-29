package testutil

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// Re-exports from pkg/scenario/actions for backward compatibility.
var (
	ExtractGRPCErrorInfo = actions.ExtractGRPCErrorInfo

	// Action builders.
	CreateLedgerAction                 = actions.CreateLedgerAction
	DeleteLedgerAction                 = actions.DeleteLedgerAction
	CreateTransactionAction            = actions.CreateTransactionAction
	CreateForceTransactionAction       = actions.CreateForceTransactionAction
	CreateForceScriptTransactionAction = actions.CreateForceScriptTransactionAction
	CreateScriptTransactionAction      = actions.CreateScriptTransactionAction
	AddAccountTypeAction               = actions.AddAccountTypeAction
	RemoveAccountTypeAction            = actions.RemoveAccountTypeAction
	SaveAccountMetadataAction          = actions.SaveAccountMetadataAction
	DeleteAccountMetadataAction        = actions.DeleteAccountMetadataAction
	SaveTransactionMetadataAction      = actions.SaveTransactionMetadataAction
	DeleteTransactionMetadataAction    = actions.DeleteTransactionMetadataAction
	RevertTransactionAction            = actions.RevertTransactionAction
	WithTimestamp                      = actions.WithTimestamp
	WithExpandVolumes                  = actions.WithExpandVolumes
	NewPosting                         = actions.NewPosting
	RegisterSigningKeyAction           = actions.RegisterSigningKeyAction
	RevokeSigningKeyAction             = actions.RevokeSigningKeyAction
	SetSigningConfigAction             = actions.SetSigningConfigAction
	FindSigningKey                     = actions.FindSigningKey
	FindMetadataValue                  = actions.FindMetadataValue
	ClosePeriodAction                  = actions.ClosePeriodAction
	SetMaintenanceModeAction           = actions.SetMaintenanceModeAction
	SetPeriodScheduleAction            = actions.SetPeriodScheduleAction
	DeletePeriodScheduleAction         = actions.DeletePeriodScheduleAction
	SetMetadataFieldTypeAction         = actions.SetMetadataFieldTypeAction
	RemoveMetadataFieldTypeAction      = actions.RemoveMetadataFieldTypeAction
	CreateLedgerWithSchemaAction       = actions.CreateLedgerWithSchemaAction
	SaveTypedAccountMetadataAction     = actions.SaveTypedAccountMetadataAction
	SaveTypedTransactionMetadataAction = actions.SaveTypedTransactionMetadataAction
	SaveNumscriptAction                = actions.SaveNumscriptAction
	SaveNumscriptWithVersionAction     = actions.SaveNumscriptWithVersionAction
	DeleteNumscriptAction              = actions.DeleteNumscriptAction
	CreateScriptRefTransactionAction   = actions.CreateScriptRefTransactionAction
	CreateBuiltinTxIndexAction         = actions.CreateBuiltinTxIndexAction
	DropBuiltinTxIndexAction           = actions.DropBuiltinTxIndexAction
	CreateAccountMetadataIndexAction   = actions.CreateAccountMetadataIndexAction
	DropAccountMetadataIndexAction     = actions.DropAccountMetadataIndexAction
	ArchivePeriodAction                = actions.ArchivePeriodAction
	GetCreatedTransactionID            = actions.GetCreatedTransactionID

	// Filter builders.
	StringMetadataFilter          = actions.StringMetadataFilter
	AddressPrefixFilter           = actions.AddressPrefixFilter
	AddressExactFilter            = actions.AddressExactFilter
	ReferenceFilter               = actions.ReferenceFilter
	AndFilter                     = actions.AndFilter
	OrFilter                      = actions.OrFilter
	NotFilter                     = actions.NotFilter
	LedgerFilter                  = actions.LedgerFilter
	ParamAddressPrefixFilter      = actions.ParamAddressPrefixFilter
	ParamAddressExactFilter       = actions.ParamAddressExactFilter
	ParamStringMetadataFilter     = actions.ParamStringMetadataFilter
	ParamBoolMetadataFilter       = actions.ParamBoolMetadataFilter
	ParamInt64RangeMetadataFilter = actions.ParamInt64RangeMetadataFilter
	Int64RangeMetadataFilter      = actions.Int64RangeMetadataFilter
	BoolMetadataFilter            = actions.BoolMetadataFilter
	StringParam                   = actions.StringParam
	Int64Param                    = actions.Int64Param
	Uint64Param                   = actions.Uint64Param
	BoolParam                     = actions.BoolParam

	// Read helpers.
	ListLedgers                    = actions.ListLedgers
	ListNumscripts                 = actions.ListNumscripts
	ListAllAccounts                = actions.ListAllAccounts
	ListAllTransactions            = actions.ListAllTransactions
	ListAllLogs                    = actions.ListAllLogs
	ListAllPeriods                 = actions.ListAllPeriods
	GetAccount                     = actions.GetAccount
	GetTransaction                 = actions.GetTransaction
	GetLedger                      = actions.GetLedger
	GetLedgerStats                 = actions.GetLedgerStats
	GetNumscript                   = actions.GetNumscript
	AggregateVolumes               = actions.AggregateVolumes
	ListAuditEntries               = actions.ListAuditEntries
	GetMetadataSchemaStatus        = actions.GetMetadataSchemaStatus
	GetPeriodSchedule              = actions.GetPeriodSchedule
	AnalyzeAccounts                = actions.AnalyzeAccounts
	AnalyzeTransactions            = actions.AnalyzeTransactions
	GetLog                         = actions.GetLog
	GetAuditEntry                  = actions.GetAuditEntry
	Discovery                      = actions.Discovery
	GetPrimaryMetrics              = actions.GetPrimaryMetrics
	GetSecondaryMetrics            = actions.GetSecondaryMetrics
	GetIndexStatus                 = actions.GetIndexStatus
	ListAccountsFiltered           = actions.ListAccountsFiltered
	ListTransactionsFiltered       = actions.ListTransactionsFiltered
	CreatePreparedQuery            = actions.CreatePreparedQuery
	UpdatePreparedQuery            = actions.UpdatePreparedQuery
	DeletePreparedQuery            = actions.DeletePreparedQuery
	ListPreparedQueries            = actions.ListPreparedQueries
	ExecutePreparedQuery           = actions.ExecutePreparedQuery
	ExecutePreparedQueryWithParams = actions.ExecutePreparedQueryWithParams

	// Backup helpers.
	CollectCheckStoreEvents = actions.CollectCheckStoreEvents
)

// Type re-exports for backward compatibility.
type (
	CheckStoreResult = actions.CheckStoreResult
)
