package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
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
