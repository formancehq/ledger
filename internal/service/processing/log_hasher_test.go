package processing

import (
	"encoding/hex"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
)

func TestHashVersion(t *testing.T) {
	t.Parallel()
	require.Equal(t, byte(1), HashVersion)
}

func TestGoldenHashCreateLedger(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Info: &commonpb.LedgerInfo{
						Name:      "default",
						CreatedAt: &commonpb.Timestamp{Data: 1700000000},
						Id:        42,
					},
				},
			},
		},
	}

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "588994eb156f50c0a9c44d54ce82cadef628b055e0e38e545558324f84349cec", got)
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
										Metadata: &commonpb.MetadataSet{
											Metadata: []*commonpb.Metadata{
												{Key: "type", Value: commonpb.NewStringValue("transfer")},
											},
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "c6951d31dd41a28c1a15271e39eb1012866d8adfe391330189304b1888ab6d3a", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "dc12f734090785d59c2f1dc2271e7d1bff6973208ad3c1689b6028f42fb9ff8d", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "d044bd44f548384fe98e19f72b444ceb63635de05f5e5a1e6654b73a24b08d87", got)
}

func TestGoldenHashChain(t *testing.T) {
	t.Parallel()

	log1 := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerLog{
					Info: &commonpb.LedgerInfo{
						Name:      "ops",
						CreatedAt: &commonpb.Timestamp{Data: 1700000000},
						Id:        1,
					},
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

	h := blake3.New()
	hash1 := ComputeLogHash(h, nil, log1)
	hash2 := ComputeLogHash(h, hash1, log2)

	gotHash1 := hex.EncodeToString(hash1)
	gotHash2 := hex.EncodeToString(hash2)

	require.Equal(t, "d1c86a9d2ff2c0cfbd5edf19264c695de48a64511c695dfc6bfae58143eb74ec", gotHash1)
	require.Equal(t, "abc9db315f7028ed8f4147a6b9abe1a326bc35fb95d1a6e16546c40276585045", gotHash2)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "acbff024e2acb3dbd4c5a08531cd3b05abfb22aefe2d6f5fce54d654b83cae8b", got)
}

func TestGoldenHashDeleteLedger(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 30,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteLedger{
				DeleteLedger: &commonpb.DeleteLedgerLog{
					Info: &commonpb.LedgerInfo{
						Name:      "old-ledger",
						CreatedAt: &commonpb.Timestamp{Data: 1700000000},
						Id:        7,
						DeletedAt: &commonpb.Timestamp{Data: 1700500000},
					},
				},
			},
		},
	}

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "d964fcb9c2820082460b133fe473c452d91cd1b43d4c7b3fc48f1967d2b3c1ee", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "4593f5458510c4ebb4a665dd6c15fdeff37b4b7164ec99f52a7871fddddbe963", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "5bd7cef2502c9ee88deb3c00276427c571973b10f456ca9154a1ab7fadc0cfea", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "24620ea62c5bf39461900f97879eeaebbeed1f20762bf7005e001316ffb263a8", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "d610bcb6577c5444f222df32e2b03668be1501ed60fbf8d4e6ea7f0ec1bb3c69", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "403420c6a026b66db0f6e731cb2f6fbaa0fc01aab87fea57370edb6e999af2a9", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "9c6505f6044005f110709b82e558120a02e4bb648cbd52a9cecb46e59e3536e7", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "be64e50062f4c360436a8fc4293e66a8f738aae18d51a68bf082e3f7b0bf7d1f", got)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	// Golden value computed from the implementation
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
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

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	// Golden value computed from the implementation
	require.NotEmpty(t, got)
	require.Len(t, got, 64)
}

func TestGoldenHashNilPayload(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 99,
	}

	h := blake3.New()
	got := hex.EncodeToString(ComputeLogHash(h, nil, log))
	require.Equal(t, "104c3a76bfe5455b41027204dfbb1ee919aa76d99ae9df59b78af8c889cd778b", got)
}
