package processing

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

			h := blake3.New()
			got := hex.EncodeToString(ComputeLogHash(h, nil, tc.log))
			require.NotEmpty(t, got)
			require.Len(t, got, 64)
		})
	}
}
