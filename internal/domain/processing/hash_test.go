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
	require.Equal(t, "4c3a6bfe05fec9cba8609d5ef3ec2ee8bd1900ad07c6848177954dee2da4a74d", got)
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
	require.Equal(t, "d93e91d45ac362c62d7b60b0b339c424f53284bbde9cf8e49fdc04dd7a7d6393", got)
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
	require.Equal(t, "f04073764b0943de1bc9a2d3f029617859b870fb980a6246f962c8dbc7597281", got)
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
	require.Equal(t, "156dc03db4090c12f312d91932f8f2960cf50358072cb971a967bdfaa8099bc3", got)
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

	require.Equal(t, "1b39681428080739048fb5c4c10c7a7ee8a8457b7900b2738806f5512a73f04a", gotHash1)
	require.Equal(t, "9d0ef52b62bea8aa341a09ced470247d53fdcb707d3f3b631109cf30b625f22f", gotHash2)
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
	require.Equal(t, "0be9ea6b42614612fed58b404e15492d3749932e354fcfbcd08d75fceea5f603", got)
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
	require.Equal(t, "5ecb640d59b7ccf95196cfe150254ca6c26b92bed122457586ef41df10d44a80", got)
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
	require.Equal(t, "0a3c5bbfb95e0cb05ab69604378e4efb2c7d5ff8e793802cf1b4a00fdbf3deff", got)
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
	require.Equal(t, "46499e88a7e3af506d10420fb1afefebc988906e3900c193a53fb41731243ac4", got)
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
	require.Equal(t, "8a4b0ef9a4cfbb44888488d1ef4a81eac64ac400d0b85b46884c7753b6e1d072", got)
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
	require.Equal(t, "4dfaa042ee56fb64425efde50eecc866c68ac069d1c99c25eec434dde2b0bc8a", got)
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
	require.Equal(t, "57ebde89fce9b769b7aa8d05e51407e1019893290c5f5545c83162071f24ab08", got)
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
	require.Equal(t, "48d6b6508309e58f9f8261a305145c42c39258efd8d69f9969d5460f0bd01da6", got)
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
	require.Equal(t, "1cae9c8d4b4a448b5dd90d8a96a03a49cd404b5d9e69c62492bf24057d87cedf", got)
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
	require.Equal(t, "9b33ffed56648e735e783c97c28910b06f8d2aa1fda191d07da2556a8a922ade", got)
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
	require.Equal(t, "ec1fa38ae983e8a790e721aebc26fe43f389bb561a7e1d38cb408c3d7e3ff50c", got)
}

func TestGoldenHashNilPayload(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 99,
	}

	_, hashResult := computeLogHash(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, nil, nil, nil, log)
	got := hex.EncodeToString(hashResult)
	require.Equal(t, "694ffc71c585ffe6e555885384da6e05cd990c9ed138c0ca48525abb1c7ce5fd", got)
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
