package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestValidateOrderKVPolicy(t *testing.T) {
	t.Parallel()
	policy := &commonpb.ClusterPolicy{KvMaxKeyBytes: 2, KvMaxValueBytes: 4}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_PutKv{PutKv: &raftcmdpb.PutKVOrder{Key: "ab", Value: []byte{0, 1, 2, 3}}},
	}}}
	require.Nil(t, validateOrderKVPolicy(order, policy))
	order.GetLedgerScoped().GetPutKv().Value = append(order.GetLedgerScoped().GetPutKv().Value, 4)
	require.Error(t, validateOrderKVPolicy(order, policy))
	order.GetLedgerScoped().GetPutKv().Value = nil
	order.GetLedgerScoped().GetPutKv().Key = "abc"
	require.Error(t, validateOrderKVPolicy(order, policy))
}
