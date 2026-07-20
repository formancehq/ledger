package domain

import "github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"

// AuditOrderType returns the stable token used to index and filter an audit
// item by its order kind. The vocabulary is the contract shared with the
// audit filter DSL (EN-1305); extend it additively, never rename a token.
func AuditOrderType(order *raftcmdpb.Order) string {
	switch t := order.GetType().(type) {
	case *raftcmdpb.Order_LedgerScoped:
		return ledgerScopedOrderType(t.LedgerScoped)
	case *raftcmdpb.Order_SystemScoped:
		return systemScopedOrderType(t.SystemScoped)
	default:
		return "unknown"
	}
}

func ledgerScopedOrderType(o *raftcmdpb.LedgerScopedOrder) string {
	switch a := o.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_Apply:
		return ledgerApplyOrderType(a.Apply)
	case *raftcmdpb.LedgerScopedOrder_CreateLedger:
		return "create_ledger"
	case *raftcmdpb.LedgerScopedOrder_DeleteLedger:
		return "delete_ledger"
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		return "mirror_ingest"
	case *raftcmdpb.LedgerScopedOrder_PromoteLedger:
		return "promote_ledger"
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		return "save_ledger_metadata"
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		return "delete_ledger_metadata"
	case *raftcmdpb.LedgerScopedOrder_SaveNumscript:
		return "save_numscript"
	case *raftcmdpb.LedgerScopedOrder_CreatePreparedQuery:
		return "create_prepared_query"
	case *raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery:
		return "update_prepared_query"
	case *raftcmdpb.LedgerScopedOrder_DeletePreparedQuery:
		return "delete_prepared_query"
	default:
		return "unknown"
	}
}

func ledgerApplyOrderType(o *raftcmdpb.LedgerApplyOrder) string {
	switch o.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		return "create_transaction"
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		return "revert_transaction"
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		return "add_metadata"
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		return "delete_metadata"
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		return "set_metadata_field_type"
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		return "remove_metadata_field_type"
	case *raftcmdpb.LedgerApplyOrder_CreateIndex:
		return "create_index"
	case *raftcmdpb.LedgerApplyOrder_DropIndex:
		return "drop_index"
	case *raftcmdpb.LedgerApplyOrder_AddAccountType:
		return "add_account_type"
	case *raftcmdpb.LedgerApplyOrder_RemoveAccountType:
		return "remove_account_type"
	case *raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode:
		return "update_default_enforcement_mode"
	default:
		return "unknown"
	}
}

func systemScopedOrderType(o *raftcmdpb.SystemScopedOrder) string {
	switch o.GetPayload().(type) {
	case *raftcmdpb.SystemScopedOrder_RegisterSigningKey:
		return "register_signing_key"
	case *raftcmdpb.SystemScopedOrder_RevokeSigningKey:
		return "revoke_signing_key"
	case *raftcmdpb.SystemScopedOrder_SetSigningConfig:
		return "set_signing_config"
	case *raftcmdpb.SystemScopedOrder_AddEventsSink:
		return "add_events_sink"
	case *raftcmdpb.SystemScopedOrder_RemoveEventsSink:
		return "remove_events_sink"
	case *raftcmdpb.SystemScopedOrder_CloseChapter:
		return "close_chapter"
	case *raftcmdpb.SystemScopedOrder_SealChapter:
		return "seal_chapter"
	case *raftcmdpb.SystemScopedOrder_ArchiveChapter:
		return "archive_chapter"
	case *raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter:
		return "confirm_archive_chapter"
	case *raftcmdpb.SystemScopedOrder_SetMaintenanceMode:
		return "set_maintenance_mode"
	case *raftcmdpb.SystemScopedOrder_SetChapterSchedule:
		return "set_chapter_schedule"
	case *raftcmdpb.SystemScopedOrder_DeleteChapterSchedule:
		return "delete_chapter_schedule"
	case *raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint:
		return "create_query_checkpoint"
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint:
		return "delete_query_checkpoint"
	case *raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule:
		return "set_query_checkpoint_schedule"
	case *raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule:
		return "delete_query_checkpoint_schedule"
	default:
		return "unknown"
	}
}
