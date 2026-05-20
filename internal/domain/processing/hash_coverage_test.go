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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "11c884cb920d451c85c00d0a588ce0b6affc23620fafd15716e89ce49cc058bb", got)
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
									Metadata: map[string]*commonpb.MetadataValue{
										"status": commonpb.NewStringValue("active"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "4e6be16ea75450d8143f2f58d1a343ee752164bb3b5d5c8db159a828ed5ddf37", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "be50e75c5fdf7c8025fbba2019c5c9c8ca570a61c3ca1dd5ea302948dff4eebb", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "882e4eb88286ccb3cbfda83a9d16033862287aeea64d288ede0aa00ef8f609f2", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "ab35cebe66064b348d06d1e8fddbc2ec3cb9e512bd4df6f65e2e342afa999b1e", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "f1039526dae38cc313585cce9f2d178375675a5eb0cadc6df4108c76a136d88e", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "c614d5c6c7d417e5a50beb327b130381939b61078131783ddcea486de21a63a1", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "1517017f74752f3073863836ca1d0258ec3ddece8f39ace457dc4c663c4b123b", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "3e69e88a510c926dfeb1dd3093de08301bd7caac2e14db619b3922aaecc8abc8", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "0970146048716837ccdd519aa2b4a449eef423f8d74409c6f099dc1b92dc6f14", got)
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

			_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, tc.log)
			got := hex.EncodeToString(hashResult)
			require.NotEmpty(t, got)
			require.Len(t, got, 64)
		})
	}
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "ea391f1dc496e5889aaa567a663b4cdb6a1034675d709aead508c22987287ff2", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "af456396e806aefa641b20b9445ad841760e44cc42c777e4d7e1046bd3faefc7", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "d1eb7185fc8dd4dfdd5dded9e5d847ac7d72c68d9bd94824ed7f213374f76fba", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "c8f33c8914815f495ed54f12ccb4b0c50b83799b3787a60762e92eb79dd0b281", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "b31496fb297f64b9a813ba0e87e06f76b9927b26014901834189575c274b9641", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "e9a0aad8a32da10aa572ddf31db63a36ef03852e9a500fe59436e46efe2b0026", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "d19871295138c46b1ea9401a56ba08790c4c1bcf5f16626428e0f0e48aa5c685", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "cc290ba7545e963d4ecd4b057989266361fd3bf18dda0b84603d431825758402", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "f2777dcc8a713dc669538d70aa72f95dbbd6ce8a44ec82b2467d9a091986dd6c", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "cc07d0c981655c4559e7d6ed46fc8f05b1ffc31cf035d50ac32b5b91e6a4275f", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "e7b32ee61ae220325f5509c39c4f4f1286e8f24c46352cd8dc5c661c3e116b15", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "52fc5b8a3332f86093d8eca5b96ef0727ec63ff87745b1360b74447f895637a0", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "ac025f278ce97d110cb4e6a864f2ed31f0d476caf23519b4720ff578086775d4", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "5678667be0edc8ed25ff1713de9d85e348b2fd548dd1bfc032977c3235308e48", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "eebf38243cf56cddfedfa96629cb3839cd90c011cb5484153266688230bf4db0", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "689a8fdd04c12d315c00e1ac534c16ed3758962afcf558627251950e820938dd", got)
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

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "7c3a4f59485c4dc7507b44414e621d1cbbaa5de349689a0c6047658d614cf622", got)
}
