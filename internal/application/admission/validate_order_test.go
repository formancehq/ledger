package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestValidateOrder_LedgerName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		order   *raftcmdpb.Order
		wantErr error
	}{
		{
			name: "valid CreateLedger",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "default"},
				},
			},
		},
		{
			name: "null byte in CreateLedger name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "ledger\x00evil"},
				},
			},
			wantErr: domain.ErrLedgerNameContainsNullByte,
		},
		{
			name: "empty CreateLedger name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: ""},
				},
			},
			wantErr: domain.ErrLedgerNameRequired,
		},
		{
			name: "null byte in Apply ledger",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Ledger: "bad\x00name"},
				},
			},
			wantErr: domain.ErrLedgerNameContainsNullByte,
		},
		{
			name: "null byte in SaveNumscript ledger",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SaveNumscript{
					SaveNumscript: &raftcmdpb.SaveNumscriptOrder{Ledger: "ns\x00bad"},
				},
			},
			wantErr: domain.ErrLedgerNameContainsNullByte,
		},
		{
			name: "valid order without ledger (ClosePeriod)",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_ClosePeriod{
					ClosePeriod: &raftcmdpb.ClosePeriodOrder{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOrder(tt.order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateOrder_MetadataKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		order   *raftcmdpb.Order
		wantErr error
	}{
		{
			name: "valid metadata in CreateTransaction",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Metadata: map[string]*commonpb.MetadataValue{
									"category": {Type: &commonpb.MetadataValue_StringValue{StringValue: "test"}},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "null byte in CreateTransaction metadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Metadata: map[string]*commonpb.MetadataValue{
									"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyContainsNullByte,
		},
		{
			name: "null byte in SaveLedgerMetadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SaveLedgerMetadata{
					SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{
						Ledger: "default",
						Metadata: map[string]*commonpb.MetadataValue{
							"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyContainsNullByte,
		},
		{
			name: "empty metadata key in DeleteMetadata",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
							DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{Key: ""},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyEmpty,
		},
		{
			name: "null byte in account metadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								AccountMetadata: map[string]*commonpb.MetadataMap{
									"users:001": {
										Values: map[string]*commonpb.MetadataValue{
											"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
										},
									},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyContainsNullByte,
		},
		// #322: revert orders silently bypassed validateApplyMetadataKeys
		// (no case for LedgerApplyOrder_RevertTransaction). A client could
		// thus ship empty / NUL-bearing keys directly into the canonical
		// Pebble layout via the revert log.
		{
			name: "null byte in RevertTransaction metadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								Metadata: map[string]*commonpb.MetadataValue{
									"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyContainsNullByte,
		},
		{
			name: "empty metadata key in RevertTransaction",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								Metadata: map[string]*commonpb.MetadataValue{
									"": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyEmpty,
		},
		{
			name: "valid metadata in RevertTransaction",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								Metadata: map[string]*commonpb.MetadataValue{
									"reason": {Type: &commonpb.MetadataValue_StringValue{StringValue: "wrong amount"}},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOrder(tt.order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateOrder_MetadataValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		order   *raftcmdpb.Order
		wantErr error
	}{
		{
			name: "null byte in CreateTransaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Metadata: map[string]*commonpb.MetadataValue{
									"category": commonpb.NewStringValue("safe\x00poison"),
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in CreateTransaction account metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								AccountMetadata: map[string]*commonpb.MetadataMap{
									"users:001": {
										Values: map[string]*commonpb.MetadataValue{
											"role": commonpb.NewStringValue("admin\x00poison"),
										},
									},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in AddMetadata null original",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
							AddMetadata: &raftcmdpb.SaveMetadataOrder{
								Metadata: map[string]*commonpb.MetadataValue{
									"score": commonpb.NewNullValue("not\x00numeric"),
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in RevertTransaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
							RevertTransaction: &raftcmdpb.RevertTransactionOrder{
								TransactionId: 1,
								Metadata: map[string]*commonpb.MetadataValue{
									"reason": commonpb.NewStringValue("bad\x00reason"),
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in SaveLedgerMetadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SaveLedgerMetadata{
					SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{
						Ledger: "default",
						Metadata: map[string]*commonpb.MetadataValue{
							"region": commonpb.NewStringValue("eu\x00west"),
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest created transaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Ledger: "default",
						Entry: &raftcmdpb.MirrorLogEntry{
							Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
								CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
									Metadata: map[string]*commonpb.MetadataValue{
										"category": commonpb.NewStringValue("safe\x00poison"),
									},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest account metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Ledger: "default",
						Entry: &raftcmdpb.MirrorLogEntry{
							Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
								CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
									AccountMetadata: map[string]*commonpb.MetadataMap{
										"users:001": {
											Values: map[string]*commonpb.MetadataValue{
												"role": commonpb.NewStringValue("admin\x00poison"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest saved metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Ledger: "default",
						Entry: &raftcmdpb.MirrorLogEntry{
							Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
								SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
									Metadata: map[string]*commonpb.MetadataValue{
										"status": commonpb.NewStringValue("active\x00poison"),
									},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest reverted transaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Ledger: "default",
						Entry: &raftcmdpb.MirrorLogEntry{
							Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
								RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
									Metadata: map[string]*commonpb.MetadataValue{
										"reason": commonpb.NewStringValue("bad\x00reason"),
									},
								},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "valid typed metadata values",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Ledger: "default",
						Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
							AddMetadata: &raftcmdpb.SaveMetadataOrder{
								Metadata: map[string]*commonpb.MetadataValue{
									"name":   commonpb.NewStringValue("alice"),
									"age":    commonpb.NewIntValue(42),
									"active": commonpb.NewBoolValue(true),
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOrder(tt.order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
