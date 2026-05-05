package processing

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// TestGoldenHashRevertedTransaction covers hashRevertedTransaction and hashTarget.
func TestGoldenHashRevertedTransaction(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 40,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   2,
						Date: &commonpb.Timestamp{Data: 1700000200},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
								RevertedTransaction: &commonpb.RevertedTransaction{
									RevertedTransactionId: 1,
									RevertTransaction: &commonpb.Transaction{
										Id:        2,
										Timestamp: &commonpb.Timestamp{Data: 1700000200},
										Postings: []*commonpb.Posting{
											{
												Source:      "users:001",
												Destination: "world",
												Amount:      &commonpb.Uint256{V0: 100},
												Asset:       "USD/2",
											},
										},
										InsertedAt: &commonpb.Timestamp{Data: 1700000200},
										UpdatedAt:  &commonpb.Timestamp{Data: 1700000200},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "87c1190c86a2ccdb76a9ee7e64d14c364bbb83acccb34396e3c1a2ee3d764440", got)
}

// TestGoldenHashSavedMetadata covers hashSavedMetadata and hashTarget (account).
func TestGoldenHashSavedMetadata(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 41,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   3,
						Date: &commonpb.Timestamp{Data: 1700000300},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SavedMetadata{
								SavedMetadata: &commonpb.SavedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{
											Account: &commonpb.TargetAccount{Addr: "user:123"},
										},
									},
									Metadata: &commonpb.MetadataSet{
										Metadata: []*commonpb.Metadata{
											{Key: "status", Value: commonpb.NewStringValue("active")},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "cf60a55a14e2b846fd836c76776f346484684cdf4ed7f0a8f72a6eb846a1cc8e", got)
}

// TestGoldenHashDeletedMetadata covers hashDeletedMetadata and hashTarget (transaction).
func TestGoldenHashDeletedMetadata(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 42,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   4,
						Date: &commonpb.Timestamp{Data: 1700000400},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
								DeletedMetadata: &commonpb.DeletedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Transaction{
											Transaction: &commonpb.TargetTransaction{Id: 5},
										},
									},
									Key: "some-key",
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "bddaafced883666a640e0cee69d804d55adca65d63719a5cc4ccbb867c4e5b24", got)
}

// TestGoldenHashSetMetadataFieldType covers hashSetMetadataFieldType.
func TestGoldenHashSetMetadataFieldType(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 43,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   5,
						Date: &commonpb.Timestamp{Data: 1700000500},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
								SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
									TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
									Key:        "age",
									Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "cc30616ca2bf1f5ad32bbe43e7591b6bfe1cfa4e97d4d32525c7f14366b835b7", got)
}

// TestGoldenHashRemovedMetadataFieldType covers hashRemovedMetadataFieldType.
func TestGoldenHashRemovedMetadataFieldType(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 44,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   6,
						Date: &commonpb.Timestamp{Data: 1700000600},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
								RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{
									TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
									Key:        "category",
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "08c9d9062714d93436fd3fb6f1ce11d78bf7721c378377f50d48524ea53974d5", got)
}

// TestGoldenHashConvertMetadataBatch covers hashConvertMetadataBatchLog.
func TestGoldenHashConvertMetadataBatch(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 45,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   7,
						Date: &commonpb.Timestamp{Data: 1700000700},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_ConvertMetadataBatch{
								ConvertMetadataBatch: &commonpb.ConvertMetadataBatchLog{
									TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
									Key:        "age",
									Count:      42,
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "fe4e4c2aa77f90b57103854c9e47d222b9e45463a42014dad4e2c7086688e42f", got)
}

// TestGoldenHashMetadataConversionComplete covers hashMetadataConversionCompleteLog.
func TestGoldenHashMetadataConversionComplete(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 46,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   8,
						Date: &commonpb.Timestamp{Data: 1700000800},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_MetadataConversionComplete{
								MetadataConversionComplete: &commonpb.MetadataConversionCompleteLog{
									TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
									Key:        "age",
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "ccf2431806499b41094b95f70f6ec187946c20a7fe19158329c9fba578178cba", got)
}

// TestGoldenHashNatsSinkConfig covers hashSinkConfig with NATS type.
func TestGoldenHashNatsSinkConfig(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 47,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_AddedEventsSink{
				AddedEventsSink: &commonpb.AddedEventsSinkLog{
					Config: &commonpb.SinkConfig{
						Name: "my-nats",
						Type: &commonpb.SinkConfig_Nats{
							Nats: &commonpb.NatsSinkConfig{
								Url:   "nats://localhost:4222",
								Topic: "events",
							},
						},
						Format: "json",
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "3bed8097a8cea3df4f3b61c1c4400dde5456c1e7951b72a5e87d3b2f91ff1ddc", got)
}

// TestGoldenHashClickHouseSinkConfig covers hashSinkConfig with ClickHouse type.
func TestGoldenHashClickHouseSinkConfig(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 48,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_AddedEventsSink{
				AddedEventsSink: &commonpb.AddedEventsSinkLog{
					Config: &commonpb.SinkConfig{
						Name: "my-clickhouse",
						Type: &commonpb.SinkConfig_Clickhouse{
							Clickhouse: &commonpb.ClickHouseSinkConfig{
								Dsn:   "clickhouse://localhost:9000",
								Table: "events",
							},
						},
						Format: "json",
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "7fac4813c525f66514b73d4611868c7a7f096da3c67a30e8d01f89ab26667b0b", got)
}

// TestGoldenHashHttpSinkConfig covers hashSinkConfig with HTTP type.
func TestGoldenHashHttpSinkConfig(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 49,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_AddedEventsSink{
				AddedEventsSink: &commonpb.AddedEventsSinkLog{
					Config: &commonpb.SinkConfig{
						Name: "my-webhook",
						Type: &commonpb.SinkConfig_Http{
							Http: &commonpb.HttpSinkConfig{
								Endpoint: "https://example.com/webhook",
								Secret:   "my-secret",
							},
						},
						Format: "json",
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "3e73c33ff4c38960a8efe3aae03c94eed6e19f62bae86c387ea5debb3d0426dc", got)
}

// TestGoldenHashNilSubMessages verifies that nil sub-messages produce a
// deterministic hash (covering the "writePresence(false)" branches).
func TestGoldenHashNilSubMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		log  *commonpb.Log
	}{
		{
			name: "nil_create_ledger_log",
			log: &commonpb.Log{
				Sequence: 50,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: nil,
					},
				},
			},
		},
		{
			name: "nil_delete_ledger_log",
			log: &commonpb.Log{
				Sequence: 51,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_DeleteLedger{
						DeleteLedger: nil,
					},
				},
			},
		},
		{
			name: "nil_apply_log",
			log: &commonpb.Log{
				Sequence: 52,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: nil,
					},
				},
			},
		},
		{
			name: "nil_register_signing_key",
			log: &commonpb.Log{
				Sequence: 53,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_RegisterSigningKey{
						RegisterSigningKey: nil,
					},
				},
			},
		},
		{
			name: "nil_revoke_signing_key",
			log: &commonpb.Log{
				Sequence: 54,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_RevokeSigningKey{
						RevokeSigningKey: nil,
					},
				},
			},
		},
		{
			name: "nil_set_signing_config",
			log: &commonpb.Log{
				Sequence: 55,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_SetSigningConfig{
						SetSigningConfig: nil,
					},
				},
			},
		},
		{
			name: "nil_added_events_sink",
			log: &commonpb.Log{
				Sequence: 56,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_AddedEventsSink{
						AddedEventsSink: nil,
					},
				},
			},
		},
		{
			name: "nil_removed_events_sink",
			log: &commonpb.Log{
				Sequence: 57,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_RemovedEventsSink{
						RemovedEventsSink: nil,
					},
				},
			},
		},
		{
			name: "nil_close_period",
			log: &commonpb.Log{
				Sequence: 58,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_ClosePeriod{
						ClosePeriod: nil,
					},
				},
			},
		},
		{
			name: "nil_seal_period",
			log: &commonpb.Log{
				Sequence: 59,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_SealPeriod{
						SealPeriod: nil,
					},
				},
			},
		},
		{
			name: "nil_set_maintenance_mode",
			log: &commonpb.Log{
				Sequence: 60,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_SetMaintenanceMode{
						SetMaintenanceMode: nil,
					},
				},
			},
		},
		{
			name: "nil_set_period_schedule",
			log: &commonpb.Log{
				Sequence: 61,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_SetPeriodSchedule{
						SetPeriodSchedule: nil,
					},
				},
			},
		},
		{
			name: "nil_delete_period_schedule",
			log: &commonpb.Log{
				Sequence: 62,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_DeletePeriodSchedule{
						DeletePeriodSchedule: nil,
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, hashResult := ComputeLogHash(nil, nil, tc.log)
			got := hex.EncodeToString(hashResult)
			require.NotEmpty(t, got)
			require.Len(t, got, 64)
		})
	}
}

// TestGoldenHashSetAuditConfig covers LogPayload_SetAuditConfig.
func TestGoldenHashSetAuditConfig(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 70,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetAuditConfig{
				SetAuditConfig: &commonpb.SetAuditConfigLog{Enabled: true},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "20c4aabb72e82949db1599d9749e1fb6f0d95911846c4012c3a1c4221a6e8d06", got)
}

// TestGoldenHashPromoteLedger covers LogPayload_PromoteLedger.
func TestGoldenHashPromoteLedger(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 71,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_PromoteLedger{
				PromoteLedger: &commonpb.PromoteLedgerLog{
					Name: "promoted",
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "c16b2d84343f6d815d939842ac0cb503c96ed21bbebb9dc272a92131ce307a84", got)
}

// TestGoldenHashCreatedPreparedQuery covers LogPayload_CreatedPreparedQuery.
func TestGoldenHashCreatedPreparedQuery(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 72,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreatedPreparedQuery{
				CreatedPreparedQuery: &commonpb.CreatedPreparedQueryLog{
					Query: &commonpb.PreparedQuery{
						Name:   "q1",
						Ledger: "default",
						Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "4b0fc5ac7ceb24020dc909044d5460116fcaa8a3296f045437359b4ef16efb60", got)
}

// TestGoldenHashUpdatedPreparedQuery covers LogPayload_UpdatedPreparedQuery.
func TestGoldenHashUpdatedPreparedQuery(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 73,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_UpdatedPreparedQuery{
				UpdatedPreparedQuery: &commonpb.UpdatedPreparedQueryLog{
					Ledger: "default",
					Name:   "q1",
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "da8e7d8fc1ef2eb198d29f18dd84af8ebb1bfc591b8e128d6b368c3d63148e25", got)
}

// TestGoldenHashDeletedPreparedQuery covers LogPayload_DeletedPreparedQuery.
func TestGoldenHashDeletedPreparedQuery(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 74,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeletedPreparedQuery{
				DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{
					Ledger: "default",
					Name:   "q1",
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "2603b3650a26c619a8f212a22fe20fb8ddc384e2ab11c25bdbd49c2d33e773c3", got)
}

// TestGoldenHashSavedNumscript covers LogPayload_SavedNumscript.
func TestGoldenHashSavedNumscript(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 75,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SavedNumscript{
				SavedNumscript: &commonpb.SavedNumscriptLog{
					Info: &commonpb.NumscriptInfo{
						Name:    "transfer",
						Content: "send [USD/2 100] (source = @world destination = @users:001)",
						Version: "v1",
						Ledger:  "default",
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "51a65de40cfeb9adf3b37141e686edfcee6358fbbd723ee7bf2c64e39a81822b", got)
}

// TestGoldenHashDeletedNumscript covers LogPayload_DeletedNumscript.
func TestGoldenHashDeletedNumscript(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 76,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeletedNumscript{
				DeletedNumscript: &commonpb.DeletedNumscriptLog{
					Name:   "transfer",
					Ledger: "default",
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "ab867cbc315e523579aa4366d84a979af6200cd584f9e6c5b019d0918d04b107", got)
}

// TestGoldenHashCreatedQueryCheckpoint covers LogPayload_CreatedQueryCheckpoint.
func TestGoldenHashCreatedQueryCheckpoint(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 77,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
				CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
					CheckpointId: 1,
					MaxSequence:  42,
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "66e2d3ff32e95f1f9e46bfad419b33a6684c4ae5990eda787fa8a319ed84292c", got)
}

// TestGoldenHashDeletedQueryCheckpoint covers LogPayload_DeletedQueryCheckpoint.
func TestGoldenHashDeletedQueryCheckpoint(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 78,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeletedQueryCheckpoint{
				DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{
					CheckpointId: 1,
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "a08175b54275406ebf30a37ae4bfe3dc4903acaa830b34817b212d90c8d5968b", got)
}

// TestGoldenHashSetQueryCheckpointSchedule covers LogPayload_SetQueryCheckpointSchedule.
func TestGoldenHashSetQueryCheckpointSchedule(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 79,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetQueryCheckpointSchedule{
				SetQueryCheckpointSchedule: &commonpb.SetQueryCheckpointScheduleLog{
					Cron: "0 */6 * * *",
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "9970c9c5110d2e5e87b4e433ad39206e0b98afed2a03f4774135a0d267eb753e", got)
}

// TestGoldenHashDeleteQueryCheckpointSchedule covers LogPayload_DeleteQueryCheckpointSchedule.
func TestGoldenHashDeleteQueryCheckpointSchedule(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 80,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteQueryCheckpointSchedule{
				DeleteQueryCheckpointSchedule: &commonpb.DeleteQueryCheckpointScheduleLog{},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "819bbae03d0b9642dd0450db82eb651b4f523c8120e2df9198d2edf3f0b75181", got)
}

// TestGoldenHashFillGap covers LedgerLogPayload_FillGap.
func TestGoldenHashFillGap(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 81,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   20,
						Date: &commonpb.Timestamp{Data: 1700001000},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_FillGap{
								FillGap: &commonpb.FillGapLog{
									OriginalId: 7,
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "dba6a7128a96bbe705977de19250e7551b941199629aa7b8faa8c8cf0bd1bc55", got)
}

// TestGoldenHashCreateIndex covers LedgerLogPayload_CreateIndex.
func TestGoldenHashCreateIndex(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 82,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   21,
						Date: &commonpb.Timestamp{Data: 1700001100},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreateIndex{
								CreateIndex: &commonpb.CreateIndexLog{
									Index: &commonpb.CreateIndexLog_Transaction{
										Transaction: &commonpb.TransactionIndex{
											Kind: &commonpb.TransactionIndex_Builtin{
												Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "979a19c2822f7b729a0d3ee7fe6e499e6e5788807d20bf6dd9c5001d42c9d586", got)
}

// TestGoldenHashDropIndex covers LedgerLogPayload_DropIndex.
func TestGoldenHashDropIndex(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 83,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   22,
						Date: &commonpb.Timestamp{Data: 1700001200},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_DropIndex{
								DropIndex: &commonpb.DropIndexLog{
									Index: &commonpb.DropIndexLog_Transaction{
										Transaction: &commonpb.TransactionIndex{
											Kind: &commonpb.TransactionIndex_Builtin{
												Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "e915a716642f538c78f4e5ce8fdcb2b8a9a0e5969e223d43d819917b85c8486e", got)
}

// TestGoldenHashIndexReady covers LedgerLogPayload_IndexReady.
func TestGoldenHashIndexReady(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 84,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   23,
						Date: &commonpb.Timestamp{Data: 1700001300},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_IndexReady{
								IndexReady: &commonpb.IndexReadyLog{
									Index: &commonpb.IndexReadyLog_Transaction{
										Transaction: &commonpb.TransactionIndex{
											Kind: &commonpb.TransactionIndex_Builtin{
												Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "713fa14a6c7043125fb74c55ede92986f4d896ee5a1308f1a5429b08df722d5a", got)
}

// TestGoldenHashAddedAccountType covers LedgerLogPayload_AddedAccountType.
func TestGoldenHashAddedAccountType(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 85,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   24,
						Date: &commonpb.Timestamp{Data: 1700001400},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_AddedAccountType{
								AddedAccountType: &commonpb.AddedAccountTypeLog{
									AccountType: &commonpb.AccountType{
										Name:    "user-checking",
										Pattern: "users:{id}:checking",
										Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "16d37c89a51faeb56aac12f304bff6b24a72f0418bc10f2e5d097d7ac4201597", got)
}

// TestGoldenHashRemovedAccountType covers LedgerLogPayload_RemovedAccountType.
func TestGoldenHashRemovedAccountType(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 86,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   25,
						Date: &commonpb.Timestamp{Data: 1700001500},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_RemovedAccountType{
								RemovedAccountType: &commonpb.RemovedAccountTypeLog{
									Name: "user-checking",
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "9b94c09cfe6331df76fd90fdbe8edc1915c0d9e20e9d304363c1522e1eb7448b", got)
}

// TestGoldenHashUpdatedDefaultEnforcementMode covers LedgerLogPayload_UpdatedDefaultEnforcementMode.
func TestGoldenHashUpdatedDefaultEnforcementMode(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 87,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   26,
						Date: &commonpb.Timestamp{Data: 1700001600},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{
								UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{
									EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "a78ab400be721e162b4f5df9a6a34d802d742d031669e66d8574316b3f96548b", got)
}

// TestGoldenHashStartedAccountMigration covers LedgerLogPayload_StartedAccountMigration.
func TestGoldenHashStartedAccountMigration(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 88,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   27,
						Date: &commonpb.Timestamp{Data: 1700001700},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_StartedAccountMigration{
								StartedAccountMigration: &commonpb.StartedAccountMigrationLog{
									AccountTypeName: "user-checking",
									OldPattern:      "users:{id}",
									TargetPattern:   "users:{id}:checking",
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "951fbe0efa7654e169fcfeeb04a5ac22dc17e218b93766bffefedda067063d2f", got)
}

// TestGoldenHashAccountMigrationBatch covers LedgerLogPayload_AccountMigrationBatch.
func TestGoldenHashAccountMigrationBatch(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 89,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   28,
						Date: &commonpb.Timestamp{Data: 1700001800},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_AccountMigrationBatch{
								AccountMigrationBatch: &commonpb.AccountMigrationBatchLog{
									AccountTypeName: "user-checking",
									Count:           10,
									Entries: []*commonpb.AccountMigrationEntryLog{
										{
											OldAddress:   "users:001",
											NewAddress:   "users:001:checking",
											Assets:       []string{"USD/2"},
											MetadataKeys: []string{"status"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "edcf67cef8a38bdb55ff709e41a023a75ecf886ea640bb8e36768c8c76b3f581", got)
}

// TestGoldenHashCompletedAccountMigration covers LedgerLogPayload_CompletedAccountMigration.
func TestGoldenHashCompletedAccountMigration(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 90,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   29,
						Date: &commonpb.Timestamp{Data: 1700001900},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CompletedAccountMigration{
								CompletedAccountMigration: &commonpb.CompletedAccountMigrationLog{
									AccountTypeName: "user-checking",
									OldPattern:      "users:{id}",
									NewPattern:      "users:{id}:checking",
									TotalMigrated:   100,
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := ComputeLogHash(nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "4f9cfe6c1d468230b9058c0e433306b7360fb76068888a9e63618f0973c2a8ce", got)
}
