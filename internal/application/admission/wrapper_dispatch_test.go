package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const wrapperTestLedger = "books"

// TestExtractLedgerScopedNeeds_CoversEveryPayloadVariant pins the contract
// that every payload variant under LedgerScopedOrder contributes at minimum
// the wrapper ledger to the preload Needs. The wrapper-level dispatch is
// what guarantees audit attribution + admission scope after #511 — a regression
// where a new payload variant is added without a matching case here would
// silently leak it past coverage checks.
func TestExtractLedgerScopedNeeds_CoversEveryPayloadVariant(t *testing.T) {
	t.Parallel()

	ledgerKey := domain.LedgerKey{Name: wrapperTestLedger}

	cases := []struct {
		name    string
		payload *raftcmdpb.LedgerScopedOrder
		assert  func(t *testing.T, n *plan.Needs)
	}{
		{
			name: "create_ledger",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger:  wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrLedger, ledgerKey.Bytes()))
			},
		},
		{
			name: "delete_ledger",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger:  wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrLedger, ledgerKey.Bytes()))
			},
		},
		{
			name: "promote_ledger",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger:  wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrLedger, ledgerKey.Bytes()))
			},
		},
		{
			name: "mirror_ingest/created_transaction",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
					Entry: &raftcmdpb.MirrorLogEntry{
						Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
							CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
								Postings: []*commonpb.Posting{
									{Source: "world", Destination: "user:alice", Asset: "USD"},
								},
								AccountMetadata: map[string]*commonpb.MetadataMap{
									"user:alice": {Values: map[string]*commonpb.MetadataValue{
										"tag": {Type: &commonpb.MetadataValue_StringValue{StringValue: "vip"}},
									}},
								},
							},
						},
					},
				}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrLedger, ledgerKey.Bytes()))
				require.True(t, n.Has(dal.SubAttrBoundary, ledgerKey.Bytes()))
				require.True(t, n.Has(dal.SubAttrVolume, domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "world"},
					Asset:      "USD",
				}.Bytes()))
				require.True(t, n.Has(dal.SubAttrVolume, domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:alice"},
					Asset:      "USD",
				}.Bytes()))
				require.True(t, n.Has(dal.SubAttrMetadata, domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:alice"},
					Key:        "tag",
				}.Bytes()))
			},
		},
		{
			name: "mirror_ingest/saved_metadata_account",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
					Entry: &raftcmdpb.MirrorLogEntry{
						Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
							SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
								Target: &commonpb.Target{Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: "user:bob"},
								}},
								Metadata: map[string]*commonpb.MetadataValue{
									"score": {Type: &commonpb.MetadataValue_IntValue{IntValue: 42}},
								},
							},
						},
					},
				}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrMetadata, domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:bob"},
					Key:        "score",
				}.Bytes()))
			},
		},
		{
			name: "mirror_ingest/saved_metadata_transaction",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
					Entry: &raftcmdpb.MirrorLogEntry{
						Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
							SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
								Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 7}},
							},
						},
					},
				}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: wrapperTestLedger, ID: 7}.Bytes()))
			},
		},
		{
			name: "mirror_ingest/deleted_metadata_account",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
					Entry: &raftcmdpb.MirrorLogEntry{
						Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{
							DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{
								Target: &commonpb.Target{Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: "user:carol"},
								}},
								Key: "score",
							},
						},
					},
				}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrMetadata, domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:carol"},
					Key:        "score",
				}.Bytes()))
			},
		},
		{
			name: "mirror_ingest/deleted_metadata_transaction",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
					Entry: &raftcmdpb.MirrorLogEntry{
						Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{
							DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{
								Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 9}},
							},
						},
					},
				}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: wrapperTestLedger, ID: 9}.Bytes()))
			},
		},
		{
			name: "mirror_ingest/reverted_transaction",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
					Entry: &raftcmdpb.MirrorLogEntry{
						Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
							RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
								RevertedTransactionId: 11,
								ReversePostings: []*commonpb.Posting{
									{Source: "user:alice", Destination: "world", Asset: "USD"},
								},
							},
						},
					},
				}},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: wrapperTestLedger, ID: 11}.Bytes()))
				require.True(t, n.Has(dal.SubAttrVolume, domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:alice"},
					Asset:      "USD",
				}.Bytes()))
			},
		},
		{
			name: "create_prepared_query",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
					CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
						Query: &commonpb.PreparedQuery{Name: "q1"},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrLedger, ledgerKey.Bytes()))
				require.True(t, n.Has(dal.SubAttrPreparedQuery, domain.PreparedQueryKey{LedgerName: wrapperTestLedger, Name: "q1"}.Bytes()))
			},
		},
		{
			name: "update_prepared_query",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery{
					UpdatePreparedQuery: &raftcmdpb.UpdatePreparedQueryOrder{Name: "q1"},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrPreparedQuery, domain.PreparedQueryKey{LedgerName: wrapperTestLedger, Name: "q1"}.Bytes()))
			},
		},
		{
			name: "delete_prepared_query",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeletePreparedQuery{
					DeletePreparedQuery: &raftcmdpb.DeletePreparedQueryOrder{Name: "q1"},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrPreparedQuery, domain.PreparedQueryKey{LedgerName: wrapperTestLedger, Name: "q1"}.Bytes()))
			},
		},
		{
			name: "save_numscript_latest",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{
					SaveNumscript: &raftcmdpb.SaveNumscriptOrder{Name: "tx", Version: "latest"},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrNumscriptVersion, domain.NumscriptVersionKey{LedgerName: wrapperTestLedger, Name: "tx"}.Bytes()))
				// latest does not preload a specific version content
				require.Zero(t, n.Count(dal.SubAttrNumscriptContent))
			},
		},
		{
			name: "save_numscript_semver",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{
					SaveNumscript: &raftcmdpb.SaveNumscriptOrder{Name: "tx", Version: "1.2.3"},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				// semver saves preload the specific (name, version) for immutability check
				require.True(t, n.Has(dal.SubAttrNumscriptContent, domain.NumscriptEntryKey{
					LedgerName: wrapperTestLedger,
					Name:       "tx",
					Version:    "1.2.3",
				}.Bytes()))
			},
		},
		{
			name: "delete_numscript",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteNumscript{
					DeleteNumscript: &raftcmdpb.DeleteNumscriptOrder{Name: "tx"},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrNumscriptVersion, domain.NumscriptVersionKey{LedgerName: wrapperTestLedger, Name: "tx"}.Bytes()))
			},
		},
		{
			name: "save_ledger_metadata",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{
					SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{
						Metadata: map[string]*commonpb.MetadataValue{
							"owner": {Type: &commonpb.MetadataValue_StringValue{StringValue: "team"}},
						},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrLedgerMetadata, domain.LedgerMetadataKey{LedgerName: wrapperTestLedger, Key: "owner"}.Bytes()))
			},
		},
		{
			name: "delete_ledger_metadata",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{
					DeleteLedgerMetadata: &raftcmdpb.DeleteLedgerMetadataOrder{Key: "owner"},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrLedgerMetadata, domain.LedgerMetadataKey{LedgerName: wrapperTestLedger, Key: "owner"}.Bytes()))
			},
		},
		{
			name: "apply_create_transaction_with_reference",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Reference: "order-42",
								Postings: []*commonpb.Posting{
									{Source: "world", Destination: "user:dan", Asset: "EUR"},
								},
							},
						},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrReference, domain.TransactionReferenceKey{LedgerName: wrapperTestLedger, Reference: "order-42"}.Bytes()))
				require.True(t, n.Has(dal.SubAttrVolume, domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:dan"},
					Asset:      "EUR",
				}.Bytes()))
			},
		},
		{
			name: "apply_revert_transaction",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 13,
								OriginalPostings: []*commonpb.Posting{
									{Source: "world", Destination: "user:eve", Asset: "USD"},
								},
							},
						},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: wrapperTestLedger, ID: 13}.Bytes()))
				require.True(t, n.Has(dal.SubAttrVolume, domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:eve"},
					Asset:      "USD",
				}.Bytes()))
			},
		},
		{
			name: "apply_add_metadata_account",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
							AddMetadata: &raftcmdpb.SaveMetadataOrder{
								Target: &commonpb.Target{Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: "user:fran"},
								}},
								Metadata: map[string]*commonpb.MetadataValue{
									"badge": {Type: &commonpb.MetadataValue_StringValue{StringValue: "gold"}},
								},
							},
						},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrMetadata, domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:fran"},
					Key:        "badge",
				}.Bytes()))
			},
		},
		{
			name: "apply_add_metadata_transaction",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
							AddMetadata: &raftcmdpb.SaveMetadataOrder{
								Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 17}},
							},
						},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: wrapperTestLedger, ID: 17}.Bytes()))
			},
		},
		{
			name: "apply_delete_metadata_account",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
							DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
								Target: &commonpb.Target{Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: "user:gina"},
								}},
								Key: "badge",
							},
						},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrMetadata, domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: wrapperTestLedger, Account: "user:gina"},
					Key:        "badge",
				}.Bytes()))
			},
		},
		{
			name: "apply_delete_metadata_transaction",
			payload: &raftcmdpb.LedgerScopedOrder{
				Ledger: wrapperTestLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
							DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
								Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 19}},
								Key:    "score",
							},
						},
					},
				},
			},
			assert: func(t *testing.T, n *plan.Needs) {
				require.True(t, n.Has(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: wrapperTestLedger, ID: 19}.Bytes()))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			needs := plan.NewNeeds()
			extractLedgerScopedNeeds(needs, tc.payload)
			tc.assert(t, needs)
		})
	}
}

// TestExtractSystemScopedNeeds_OnlySinkConfigsContribute pins the contract
// that, of all system-scoped payloads, only sink-config writes contribute
// preload keys. Every other variant is intentionally a no-op (no cache
// preload required), so the test sweeps each variant and asserts the Needs
// stays empty.
func TestExtractSystemScopedNeeds_OnlySinkConfigsContribute(t *testing.T) {
	t.Parallel()

	t.Run("add_events_sink", func(t *testing.T) {
		t.Parallel()
		needs := plan.NewNeeds()
		extractSystemScopedNeeds(needs, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
				Config: &commonpb.SinkConfig{Name: "kafka-main"},
			}},
		})
		require.True(t, needs.Has(dal.SubAttrSinkConfig, domain.SinkConfigKey{Name: "kafka-main"}.Bytes()))
	})

	t.Run("remove_events_sink", func(t *testing.T) {
		t.Parallel()
		needs := plan.NewNeeds()
		extractSystemScopedNeeds(needs, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
				Name: "kafka-main",
			}},
		})
		require.True(t, needs.Has(dal.SubAttrSinkConfig, domain.SinkConfigKey{Name: "kafka-main"}.Bytes()))
	})

	noOpVariants := []struct {
		name    string
		payload *raftcmdpb.SystemScopedOrder
	}{
		{"register_signing_key", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{}}}},
		{"revoke_signing_key", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{}}}},
		{"set_signing_config", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetSigningConfig{SetSigningConfig: &raftcmdpb.SetSigningConfigOrder{}}}},
		{"set_maintenance_mode", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{}}}},
		{"close_chapter", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{CloseChapter: &raftcmdpb.CloseChapterOrder{}}}},
		{"seal_chapter", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SealChapter{SealChapter: &raftcmdpb.SealChapterOrder{}}}},
		{"archive_chapter", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_ArchiveChapter{ArchiveChapter: &raftcmdpb.ArchiveChapterOrder{}}}},
		{"confirm_archive_chapter", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter{ConfirmArchiveChapter: &raftcmdpb.ConfirmArchiveChapterOrder{}}}},
		{"set_chapter_schedule", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetChapterSchedule{SetChapterSchedule: &raftcmdpb.SetChapterScheduleOrder{}}}},
		{"delete_chapter_schedule", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteChapterSchedule{DeleteChapterSchedule: &raftcmdpb.DeleteChapterScheduleOrder{}}}},
		{"create_query_checkpoint", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}}},
		{"delete_query_checkpoint", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &raftcmdpb.DeleteQueryCheckpointOrder{}}}},
		{"set_query_checkpoint_schedule", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &raftcmdpb.SetQueryCheckpointScheduleOrder{}}}},
		{"delete_query_checkpoint_schedule", &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &raftcmdpb.DeleteQueryCheckpointScheduleOrder{}}}},
	}

	for _, tc := range noOpVariants {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			needs := plan.NewNeeds()
			extractSystemScopedNeeds(needs, tc.payload)
			require.Zero(t, needs.Count(dal.SubAttrSinkConfig), "%s must not contribute sink configs", tc.name)
			require.Zero(t, needs.Count(dal.SubAttrLedger), "%s must not contribute a ledger", tc.name)
		})
	}
}

// TestWrapHelpers_SetEnvelopeOnOrder pins the trivial helper invariants used
// pervasively by the request dispatch.
func TestWrapHelpers_SetEnvelopeOnOrder(t *testing.T) {
	t.Parallel()

	t.Run("ledger_scoped", func(t *testing.T) {
		t.Parallel()
		order := &raftcmdpb.Order{}
		ls := &raftcmdpb.LedgerScopedOrder{
			Ledger:  wrapperTestLedger,
			Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}},
		}
		wrapLedgerScoped(order, ls)
		require.Same(t, ls, order.GetLedgerScoped())
		require.Nil(t, order.GetSystemScoped())
	})

	t.Run("system_scoped", func(t *testing.T) {
		t.Parallel()
		order := &raftcmdpb.Order{}
		ss := &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{CloseChapter: &raftcmdpb.CloseChapterOrder{}},
		}
		wrapSystemScoped(order, ss)
		require.Same(t, ss, order.GetSystemScoped())
		require.Nil(t, order.GetLedgerScoped())
	})
}
