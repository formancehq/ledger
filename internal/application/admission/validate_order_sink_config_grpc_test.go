package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestGrpcLogSinkRequiresExplicitAuthenticatedOwner(t *testing.T) {
	t.Parallel()
	config := &commonpb.SinkConfig{Name: "log-sink", Type: &commonpb.SinkConfig_GrpcLog{GrpcLog: &commonpb.GrpcLogSinkConfig{
		ClientId: "client",
	}}}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: config}},
	}}}
	require.ErrorIs(t, validateOrderSinkConfig(order), ErrGrpcLogSinkConfigInvalid)
	config.GetGrpcLog().Owner = &commonpb.CallerIdentity{Subject: "sink-owner", Source: &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"}}
	require.NoError(t, validateOrderSinkConfig(order))
	config.GetGrpcLog().Owner = &commonpb.CallerIdentity{Source: &commonpb.CallerIdentity_KeyId{KeyId: "key-1"}}
	require.NoError(t, validateOrderSinkConfig(order), "static credentials may have an empty subject")
	config.GetGrpcLog().Owner = &commonpb.CallerIdentity{Subject: "sink-owner", Source: &commonpb.CallerIdentity_Issuer{}}
	require.ErrorIs(t, validateOrderSinkConfig(order), ErrGrpcLogSinkConfigInvalid)
}
