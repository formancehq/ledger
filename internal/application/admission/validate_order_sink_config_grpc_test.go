package admission

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestGrpcLogSinkRequiresSecretVerifier(t *testing.T) {
	t.Parallel()
	config := &commonpb.SinkConfig{Name: "log-sink", Type: &commonpb.SinkConfig_GrpcLog{GrpcLog: &commonpb.GrpcLogSinkConfig{
		ClientId: "client",
	}}}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: config}},
	}}}
	require.ErrorIs(t, validateOrderSinkConfig(order), ErrGrpcLogSinkConfigInvalid)
	config.GetGrpcLog().SecretSha256 = bytes.Repeat([]byte{1}, 32)
	require.NoError(t, validateOrderSinkConfig(order))
	config.GetGrpcLog().SecretSha256 = []byte{1}
	require.ErrorIs(t, validateOrderSinkConfig(order), ErrGrpcLogSinkConfigInvalid)
}
