package processing

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestGoldenHashCreateLedger(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Name:      "default",
					CreatedAt: &commonpb.Timestamp{Data: 1700000000},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "dbb896be5c7c694acbde9bb08c889caaf912630789bc81772f6d779cb79db771", got)
}

func TestGoldenHashApplyCreatedTransaction(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 5,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "default",
					Log: &commonpb.LedgerLog{
						Id:   1,
						Date: &commonpb.Timestamp{Data: 1700000100},
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: &commonpb.Transaction{
										Id:        1,
										Timestamp: &commonpb.Timestamp{Data: 1700000100},
										Postings: []*commonpb.Posting{
											{
												Source:      "world",
												Destination: "users:001",
												Amount:      &commonpb.Uint256{V0: 100},
												Asset:       "USD/2",
											},
										},
										Metadata: map[string]*commonpb.MetadataValue{
											"type": commonpb.NewStringValue("transfer"),
										},
										Reference:  "tx-ref-001",
										InsertedAt: &commonpb.Timestamp{Data: 1700000100},
										UpdatedAt:  &commonpb.Timestamp{Data: 1700000100},
									},
								},
							},
						},
					},
				},
			},
		},
		Idempotency: &commonpb.Idempotency{Key: "ik-001"},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "909c487d0e63c00414c0bf107e62fed47e8460fecbcd8af753639cf4523b1595", got)
}

func TestGoldenHashRegisterSigningKey(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 10,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_RegisterSigningKey{
				RegisterSigningKey: &commonpb.RegisterSigningKeyLog{
					KeyId:     "key-ed25519-001",
					PublicKey: []byte{0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "30b9a454f7df0fa382ee0e9ceed7b7a3de8ba549b2fe5f40faa3583f54e53b0f", got)
}

func TestGoldenHashClosePeriod(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 20,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_ClosePeriod{
				ClosePeriod: &commonpb.ClosePeriodLog{
					ClosedPeriod: &commonpb.Period{
						Id:            1,
						Start:         &commonpb.Timestamp{Data: 1700000000},
						End:           &commonpb.Timestamp{Data: 1700100000},
						Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
						CloseSequence: 19,
						SealingHash:   []byte{0x01, 0x02, 0x03},
						LastLogHash:   []byte{0x04, 0x05, 0x06},
						StartSequence: 1,
					},
					NewPeriod: &commonpb.Period{
						Id:            2,
						Start:         &commonpb.Timestamp{Data: 1700100000},
						Status:        commonpb.PeriodStatus_PERIOD_OPEN,
						StartSequence: 20,
					},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "2bde6d515d3f70c55d726efbc6e1280948dd05496cd642c4a82a80f8902f5f2b", got)
}

func TestGoldenHashChain(t *testing.T) {
	t.Parallel()

	log1 := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Name:      "ops",
					CreatedAt: &commonpb.Timestamp{Data: 1700000000},
				},
			},
		},
	}

	log2 := &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetMaintenanceMode{
				SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{
					Enabled: true,
				},
			},
		},
	}

	_, hash1 := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log1)
	_, hash2 := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, hash1, log2)

	gotHash1 := hex.EncodeToString(hash1)
	gotHash2 := hex.EncodeToString(hash2)

	require.Equal(t, "78b8cf873e73aed74a33b6bc2f0d0a801d4e05427d0d5a666c32593ce3afdab2", gotHash1)
	require.Equal(t, "9e6b8c771a347426339fd3da2323286d1ee9c212c5aad83a82f7de9bc13d0a72", gotHash2)
}

func TestGoldenHashAddedEventsSink(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 15,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_AddedEventsSink{
				AddedEventsSink: &commonpb.AddedEventsSinkLog{
					Config: &commonpb.SinkConfig{
						Name: "analytics",
						Type: &commonpb.SinkConfig_Kafka{
							Kafka: &commonpb.KafkaSinkConfig{
								Brokers:       []string{"kafka-1:9092", "kafka-2:9092"},
								Topic:         "ledger-events",
								Tls:           true,
								SaslMechanism: "SCRAM-SHA-256",
								SaslUsername:  "producer",
								SaslPassword:  "secret",
							},
						},
						Format:       "json",
						BatchSize:    128,
						BatchDelayMs: 50,
					},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "ff164d8de230fd1a23e86ebb5d9cb4f376bc24d3efcda1c63eebc05ec98cb74f", got)
}

func TestGoldenHashDeleteLedger(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 30,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteLedger{
				DeleteLedger: &commonpb.DeleteLedgerLog{
					Name:      "old-ledger",
					DeletedAt: &commonpb.Timestamp{Data: 1700500000},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "f197d929f90f258186f78921efbe7a4bf01e45b15ff61226cfca230963e2a086", got)
}

func TestGoldenHashRevokeSigningKey(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 31,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_RevokeSigningKey{
				RevokeSigningKey: &commonpb.RevokeSigningKeyLog{
					KeyId: "key-ed25519-001",
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "330062794947902d3bf1327edb3e14201673bdd5a92ceaf43a5c641081c91315", got)
}

func TestGoldenHashSetSigningConfig(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 32,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetSigningConfig{
				SetSigningConfig: &commonpb.SetSigningConfigLog{
					RequireSignatures: true,
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "fe35366406e12f89c11d1facd2257228e97eee58708a53098af26b57168e3258", got)
}

func TestGoldenHashRemovedEventsSink(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 33,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_RemovedEventsSink{
				RemovedEventsSink: &commonpb.RemovedEventsSinkLog{
					Name: "analytics",
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "8427133cd815e495d9ec27083916c4998925a19164863f5a5108580c7bd922bf", got)
}

func TestGoldenHashSealPeriod(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 34,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SealPeriod{
				SealPeriod: &commonpb.SealPeriodLog{
					Period: &commonpb.Period{
						Id:            1,
						Start:         &commonpb.Timestamp{Data: 1700000000},
						End:           &commonpb.Timestamp{Data: 1700100000},
						Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
						CloseSequence: 19,
						SealingHash:   []byte{0xaa, 0xbb, 0xcc, 0xdd},
						LastLogHash:   []byte{0x11, 0x22, 0x33},
						StartSequence: 1,
					},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "ef0413138e5199d8b7f2d899a4d2d563aed30612738aac4ddfd7b0786f2e9abf", got)
}

func TestGoldenHashArchivePeriod(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 35,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_ArchivePeriod{
				ArchivePeriod: &commonpb.ArchivePeriodLog{
					Period: &commonpb.Period{
						Id:            1,
						Start:         &commonpb.Timestamp{Data: 1700000000},
						End:           &commonpb.Timestamp{Data: 1700100000},
						Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
						CloseSequence: 19,
						StartSequence: 1,
					},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "f665d3f1d645ee4b55e1640526611cc31ad9422df9f0f2f92877a21b9108d781", got)
}

func TestGoldenHashConfirmArchivePeriod(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 36,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_ConfirmArchivePeriod{
				ConfirmArchivePeriod: &commonpb.ConfirmArchivePeriodLog{
					Period: &commonpb.Period{
						Id:            1,
						Start:         &commonpb.Timestamp{Data: 1700000000},
						End:           &commonpb.Timestamp{Data: 1700100000},
						Status:        commonpb.PeriodStatus_PERIOD_ARCHIVED,
						CloseSequence: 19,
						StartSequence: 1,
					},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "a5baf6a4b7c856d2a4265c64aa6ac16eb4be3bba6c2f351f610a1b0204cc392f", got)
}

func TestGoldenHashSetMaintenanceMode(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 37,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetMaintenanceMode{
				SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{
					Enabled: true,
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "e16ff13d3b4e8d3d4c34dfd269fe88d4bdca590dd640ccbca8d75fee3ddb6e14", got)
}

func TestGoldenHashSetPeriodSchedule(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 38,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetPeriodSchedule{
				SetPeriodSchedule: &commonpb.SetPeriodScheduleLog{
					Cron: "0 0 1 * *",
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "3b2cd931b91ae22f4a3d196a66e8a2a027a1e046ed2e06d1e7822d2aa03483fa", got)
}

func TestGoldenHashDeletePeriodSchedule(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 39,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeletePeriodSchedule{
				DeletePeriodSchedule: &commonpb.DeletePeriodScheduleLog{},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "bc0ba8aebc620252ec766348175e82ee56ce1aa4d5bcd71d21e800ec75b3d568", got)
}

func TestGoldenHashNilPayload(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 99,
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "32d22664264af36e4c52cd1a1dac9c68966fe776c68189f736242bfaf11184d7", got)
}

func TestGoldenHashXXH3CreateLedger(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Name:      "default",
					CreatedAt: &commonpb.Timestamp{Data: 1700000000},
				},
			},
		},
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, nil, nil, nil, log)

	// XXH3-128 produces 16 bytes (32 hex chars), different from blake3's 32 bytes (64 hex chars)
	require.Len(t, hashResult, 16)
	require.Equal(t, uint32(1), log.GetHashVersion())

	// Verify it differs from blake3
	_, blake3Hash := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	require.NotEqual(t, hashResult, blake3Hash)
}

func TestGoldenHashMixedChain(t *testing.T) {
	t.Parallel()

	// Log 1: blake3
	log1 := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Name:      "default",
					CreatedAt: &commonpb.Timestamp{Data: 1700000000},
				},
			},
		},
	}
	_, hash1 := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log1)
	require.Len(t, hash1, 32) // blake3 = 32 bytes
	require.Equal(t, uint32(0), log1.GetHashVersion())

	// Log 2: xxh3, chained to blake3 hash
	log2 := &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Name:      "second",
					CreatedAt: &commonpb.Timestamp{Data: 1700000001},
				},
			},
		},
	}
	_, hash2 := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, nil, nil, hash1, log2)
	require.Len(t, hash2, 16) // xxh3 = 16 bytes
	require.Equal(t, uint32(1), log2.GetHashVersion())

	// Verify chain: recompute using ComputeLogHashByVersion
	_, verifyHash1 := ComputeLogHashByVersion(log1.GetHashVersion(), nil, nil, log1)
	require.Equal(t, hash1, verifyHash1)

	_, verifyHash2 := ComputeLogHashByVersion(log2.GetHashVersion(), nil, hash1, log2)
	require.Equal(t, hash2, verifyHash2)
}
