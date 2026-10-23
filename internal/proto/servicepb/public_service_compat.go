package servicepb

import ledgerpb "github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

type BucketServiceClient = ledgerpb.BucketServiceClient
type BucketServiceServer = ledgerpb.BucketServiceServer
type UnimplementedBucketServiceServer = ledgerpb.UnimplementedBucketServiceServer
type UnsafeBucketServiceServer = ledgerpb.UnsafeBucketServiceServer
type BucketService_ListLedgersServer = ledgerpb.BucketService_ListLedgersServer
type BucketService_ListTransactionsServer = ledgerpb.BucketService_ListTransactionsServer
type BucketService_ListAccountsServer = ledgerpb.BucketService_ListAccountsServer
type BucketService_CheckStoreServer = ledgerpb.BucketService_CheckStoreServer
type BucketService_ListAuditEntriesServer = ledgerpb.BucketService_ListAuditEntriesServer
type BucketService_ListLogsServer = ledgerpb.BucketService_ListLogsServer
type BucketService_ListSigningKeysServer = ledgerpb.BucketService_ListSigningKeysServer
type BucketService_AnalyzeAccountsServer = ledgerpb.BucketService_AnalyzeAccountsServer
type BucketService_AnalyzeTransactionsServer = ledgerpb.BucketService_AnalyzeTransactionsServer
type BucketService_ListIndexesServer = ledgerpb.BucketService_ListIndexesServer
type BucketService_ListNumscriptsServer = ledgerpb.BucketService_ListNumscriptsServer
type BucketService_ListAuditEntriesClient = ledgerpb.BucketService_ListAuditEntriesClient
type BucketService_ListLogsClient = ledgerpb.BucketService_ListLogsClient

var NewBucketServiceClient = ledgerpb.NewBucketServiceClient
var RegisterBucketServiceServer = ledgerpb.RegisterBucketServiceServer
var BucketService_ServiceDesc = ledgerpb.BucketService_ServiceDesc
var SkippableReasonsForLedgerAction = ledgerpb.SkippableReasonsForLedgerAction
var GetLedgerActionType = ledgerpb.GetLedgerActionType

const (
	BucketService_ListLedgers_FullMethodName             = ledgerpb.BucketService_ListLedgers_FullMethodName
	BucketService_GetLedger_FullMethodName               = ledgerpb.BucketService_GetLedger_FullMethodName
	BucketService_GetAccount_FullMethodName              = ledgerpb.BucketService_GetAccount_FullMethodName
	BucketService_GetTransaction_FullMethodName          = ledgerpb.BucketService_GetTransaction_FullMethodName
	BucketService_ListTransactions_FullMethodName        = ledgerpb.BucketService_ListTransactions_FullMethodName
	BucketService_ListAccounts_FullMethodName            = ledgerpb.BucketService_ListAccounts_FullMethodName
	BucketService_Apply_FullMethodName                   = ledgerpb.BucketService_Apply_FullMethodName
	BucketService_GetPrimaryMetrics_FullMethodName       = ledgerpb.BucketService_GetPrimaryMetrics_FullMethodName
	BucketService_GetSecondaryMetrics_FullMethodName     = ledgerpb.BucketService_GetSecondaryMetrics_FullMethodName
	BucketService_CheckStore_FullMethodName              = ledgerpb.BucketService_CheckStore_FullMethodName
	BucketService_ListAuditEntries_FullMethodName        = ledgerpb.BucketService_ListAuditEntries_FullMethodName
	BucketService_GetAuditEntry_FullMethodName           = ledgerpb.BucketService_GetAuditEntry_FullMethodName
	BucketService_GetEventsSinks_FullMethodName          = ledgerpb.BucketService_GetEventsSinks_FullMethodName
	BucketService_ListLogs_FullMethodName                = ledgerpb.BucketService_ListLogs_FullMethodName
	BucketService_GetLog_FullMethodName                  = ledgerpb.BucketService_GetLog_FullMethodName
	BucketService_ListSigningKeys_FullMethodName         = ledgerpb.BucketService_ListSigningKeys_FullMethodName
	BucketService_Discovery_FullMethodName               = ledgerpb.BucketService_Discovery_FullMethodName
	BucketService_GetMetadataSchemaStatus_FullMethodName = ledgerpb.BucketService_GetMetadataSchemaStatus_FullMethodName
	BucketService_AnalyzeAccounts_FullMethodName         = ledgerpb.BucketService_AnalyzeAccounts_FullMethodName
	BucketService_AnalyzeTransactions_FullMethodName     = ledgerpb.BucketService_AnalyzeTransactions_FullMethodName
	BucketService_ListPreparedQueries_FullMethodName     = ledgerpb.BucketService_ListPreparedQueries_FullMethodName
	BucketService_ExecutePreparedQuery_FullMethodName    = ledgerpb.BucketService_ExecutePreparedQuery_FullMethodName
	BucketService_GetIndexStatus_FullMethodName          = ledgerpb.BucketService_GetIndexStatus_FullMethodName
	BucketService_GetIndex_FullMethodName                = ledgerpb.BucketService_GetIndex_FullMethodName
	BucketService_GetIndexEntryStatus_FullMethodName     = ledgerpb.BucketService_GetIndexEntryStatus_FullMethodName
	BucketService_ListIndexes_FullMethodName             = ledgerpb.BucketService_ListIndexes_FullMethodName
	BucketService_GetLedgerStats_FullMethodName          = ledgerpb.BucketService_GetLedgerStats_FullMethodName
	BucketService_AggregateVolumes_FullMethodName        = ledgerpb.BucketService_AggregateVolumes_FullMethodName
	BucketService_GetNumscript_FullMethodName            = ledgerpb.BucketService_GetNumscript_FullMethodName
	BucketService_ListNumscripts_FullMethodName          = ledgerpb.BucketService_ListNumscripts_FullMethodName
	BucketService_GetTemplateUsage_FullMethodName        = ledgerpb.BucketService_GetTemplateUsage_FullMethodName
	BucketService_ListNumscriptVersions_FullMethodName   = ledgerpb.BucketService_ListNumscriptVersions_FullMethodName
	BucketService_InspectIndex_FullMethodName            = ledgerpb.BucketService_InspectIndex_FullMethodName
	BucketService_Barrier_FullMethodName                 = ledgerpb.BucketService_Barrier_FullMethodName
)
