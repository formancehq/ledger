package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

// grpcLogStreamer adapts LedgerGrpcClient to implement raft.LogStreamer
// (which requires GetAllLogs without context)
type grpcLogStreamer struct {
	client *LedgerGrpcClient
}

func (s *grpcLogStreamer) GetAllLogs(from uint64, to uint64) (store.Cursor[*commonpb.Log], error) {
	return s.client.GetAllLogs(context.Background(), from, to)
}

type grpcLogStreamerProvider struct {
	transport *raft.DefaultTransport
}

func (p *grpcLogStreamerProvider) GetForPeer(id uint64) (raft.LogStreamer, error) {
	conn := p.transport.GetPeerConnection(id)

	return &grpcLogStreamer{
		client: NewLedgerGrpcClient(servicepb.NewLedgerServiceClient(conn)),
	}, nil
}

func GRPCLogStreamerProvider(transport *raft.DefaultTransport) *grpcLogStreamerProvider {
	return &grpcLogStreamerProvider{
		transport: transport,
	}
}
