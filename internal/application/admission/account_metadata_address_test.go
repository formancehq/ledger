package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestValidateOrder_TransactionAccountMetadataAddresses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		addresses []string
		want      error
	}{
		{"empty", []string{""}, domain.ErrAccountAddressEmpty},
		{"invalid", []string{"bad!"}, domain.ErrAccountAddressInvalidChar},
		{"competing", []string{"bad!", "", "users:alice"}, domain.ErrAccountAddressEmpty},
		{"valid", []string{"users:alice"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := make(map[string]*commonpb.MetadataMap)
			for _, address := range tc.addresses {
				metadata[address] = &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}}
			}
			order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Postings:        []*commonpb.Posting{{Source: "world", Destination: "users:alice", Asset: "USD", Amount: commonpb.NewUint256FromUint64(1)}},
						AccountMetadata: metadata,
					}},
				}},
			}}}
			if tc.want == nil {
				require.NoError(t, validateOrder(order))
			} else {
				require.ErrorIs(t, validateOrder(order), tc.want)
			}
		})
	}
}
