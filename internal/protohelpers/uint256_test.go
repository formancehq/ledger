package protohelpers

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestUint256ServerAdapter(t *testing.T) {
	t.Parallel()
	values := []*uint256.Int{
		uint256.NewInt(0),
		uint256.NewInt(1),
		uint256.NewInt(1_000_000_000),
		new(uint256.Int).SetAllOne(),
	}
	for _, value := range values {
		proto := NewUint256(value)
		var got uint256.Int
		IntoUint256(proto, &got)
		require.True(t, value.Eq(&got))
		require.Equal(t, value.Dec(), proto.Dec())

		var next ledgerpb.Uint256
		SetFromUint256(&next, value)
		IntoUint256(next.AsReader(), &got)
		require.True(t, value.Eq(&got))
	}

	var dst uint256.Int
	dst.SetUint64(42)
	IntoUint256((*ledgerpb.Uint256)(nil), &dst)
	require.True(t, dst.IsZero())
}
