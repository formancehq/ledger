package domain_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAuditOrderType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		order *raftcmdpb.Order
		want  string
	}{
		{
			name: "create transaction",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{}},
				}},
			}},
			want: "create_transaction",
		},
		{
			name: "revert transaction",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{}},
				}},
			}},
			want: "revert_transaction",
		},
		{
			name: "add metadata",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{}},
				}},
			}},
			want: "add_metadata",
		},
		{
			name: "delete metadata",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{}},
				}},
			}},
			want: "delete_metadata",
		},
		{
			name: "set metadata field type",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{}},
				}},
			}},
			want: "set_metadata_field_type",
		},
		{
			name: "remove metadata field type",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{}},
				}},
			}},
			want: "remove_metadata_field_type",
		},
		{
			name: "create index",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{}},
				}},
			}},
			want: "create_index",
		},
		{
			name: "drop index",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DropIndex{}},
				}},
			}},
			want: "drop_index",
		},
		{
			name: "add account type",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{}},
				}},
			}},
			want: "add_account_type",
		},
		{
			name: "remove account type",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{}},
				}},
			}},
			want: "remove_account_type",
		},
		{
			name: "update default enforcement mode",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode{}},
				}},
			}},
			want: "update_default_enforcement_mode",
		},
		{
			name: "create ledger",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{}},
			}},
			want: "create_ledger",
		},
		{
			name: "delete ledger",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{}},
			}},
			want: "delete_ledger",
		},
		{
			name: "mirror ingest",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{}},
			}},
			want: "mirror_ingest",
		},
		{
			name: "promote ledger",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{}},
			}},
			want: "promote_ledger",
		},
		{
			name: "save ledger metadata",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{}},
			}},
			want: "save_ledger_metadata",
		},
		{
			name: "delete ledger metadata",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{}},
			}},
			want: "delete_ledger_metadata",
		},
		{
			name: "save numscript",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{}},
			}},
			want: "save_numscript",
		},
		{
			name: "delete numscript",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_DeleteNumscript{}},
			}},
			want: "delete_numscript",
		},
		{
			name: "create prepared query",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{}},
			}},
			want: "create_prepared_query",
		},
		{
			name: "update prepared query",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery{}},
			}},
			want: "update_prepared_query",
		},
		{
			name: "delete prepared query",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_DeletePreparedQuery{}},
			}},
			want: "delete_prepared_query",
		},
		{
			name: "register signing key",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{}},
			}},
			want: "register_signing_key",
		},
		{
			name: "revoke signing key",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{}},
			}},
			want: "revoke_signing_key",
		},
		{
			name: "set signing config",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetSigningConfig{}},
			}},
			want: "set_signing_config",
		},
		{
			name: "add events sink",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{}},
			}},
			want: "add_events_sink",
		},
		{
			name: "remove events sink",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{}},
			}},
			want: "remove_events_sink",
		},
		{
			name: "close chapter",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{}},
			}},
			want: "close_chapter",
		},
		{
			name: "seal chapter",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SealChapter{}},
			}},
			want: "seal_chapter",
		},
		{
			name: "archive chapter",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_ArchiveChapter{}},
			}},
			want: "archive_chapter",
		},
		{
			name: "confirm archive chapter",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter{}},
			}},
			want: "confirm_archive_chapter",
		},
		{
			name: "set maintenance mode",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{}},
			}},
			want: "set_maintenance_mode",
		},
		{
			name: "set chapter schedule",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetChapterSchedule{}},
			}},
			want: "set_chapter_schedule",
		},
		{
			name: "delete chapter schedule",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteChapterSchedule{}},
			}},
			want: "delete_chapter_schedule",
		},
		{
			name: "create query checkpoint",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{}},
			}},
			want: "create_query_checkpoint",
		},
		{
			name: "delete query checkpoint",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint{}},
			}},
			want: "delete_query_checkpoint",
		},
		{
			name: "set query checkpoint schedule",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule{}},
			}},
			want: "set_query_checkpoint_schedule",
		},
		{
			name: "delete query checkpoint schedule",
			order: &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule{}},
			}},
			want: "delete_query_checkpoint_schedule",
		},
		{
			name:  "nil order",
			order: nil,
			want:  "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, domain.AuditOrderType(tt.order))
		})
	}
}
