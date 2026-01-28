package service

import (
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
)

type grpcLogStreamerProvider struct {
	transport *raft.DefaultTransport
}

func (p *grpcLogStreamerProvider) GetForPeer(id uint64) (raft.LogStreamer, error) {
	conn := p.transport.GetPeerConnection(id)

	return NewLedgerGrpcClient(
		ledgerpb.NewLedgerServiceClient(conn),
	), nil
}

func GRPCLogStreamerProvider(transport *raft.DefaultTransport) *grpcLogStreamerProvider {
	return &grpcLogStreamerProvider{
		transport: transport,
	}
}
