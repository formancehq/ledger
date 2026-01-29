package service

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LedgerGrpcClient implements Controller by forwarding requests via gRPC to the leader
type LedgerGrpcClient struct {
	client servicepb.LedgerServiceClient
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation
func NewLedgerGrpcClient(client servicepb.LedgerServiceClient) *LedgerGrpcClient {
	return &LedgerGrpcClient{
		client: client,
	}
}

// Apply forwards the actions via gRPC to the leader
func (g *LedgerGrpcClient) Apply(ctx context.Context, actions ...*servicepb.Action) ([]*commonpb.Log, error) {
	resp, err := g.client.Apply(ctx, &servicepb.ApplyRequest{
		Actions: actions,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}
	return resp.Logs, nil
}

func (g *LedgerGrpcClient) GetTransaction(ctx context.Context, ledgerID uint32, transactionID uint64) (*commonpb.Transaction, error) {
	return g.client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		LedgerId:      ledgerID,
		TransactionId: transactionID,
	})
}

func (g *LedgerGrpcClient) Import(ctx context.Context, ledgerID uint32, stream chan *commonpb.LedgerLog) error {
	return fmt.Errorf("import is not implemented yet")
}

func (g *LedgerGrpcClient) Export(ctx context.Context, ledgerID uint32, w ExportWriter) error {
	return fmt.Errorf("export is not implemented yet")
}

// GetAllLedgerLogs returns a cursor to iterate over all ledger logs (implements LedgerLogReader)
func (g *LedgerGrpcClient) GetAllLedgerLogs(ctx context.Context, ledgerID uint32, from uint64, to uint64) (store.Cursor[*commonpb.LedgerLog], error) {
	req := &servicepb.StreamLedgerLogsRequest{
		LedgerId: ledgerID,
		FromId:   from,
		ToId:     to, // 0 means no limit
	}

	stream, err := g.client.StreamLedgerLogs(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Canceled {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return store.NewGRPCStreamCursor(stream, func(res *servicepb.StreamLedgerLogsResponse) (*commonpb.LedgerLog, error) {
		return res.Log, nil
	}), nil
}

// GetAllLogs returns a cursor to iterate over all logs (implements LogStreamer)
func (g *LedgerGrpcClient) GetAllLogs(ctx context.Context, from uint64, to uint64) (store.Cursor[*commonpb.Log], error) {
	req := &servicepb.StreamLogsRequest{
		FromSequence: from,
		ToSequence:   to,
	}

	stream, err := g.client.StreamLogs(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Canceled {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return store.NewGRPCStreamCursor(stream, func(res *servicepb.StreamLogsResponse) (*commonpb.Log, error) {
		return res.Log, nil
	}), nil
}

func (g *LedgerGrpcClient) GetAllLedgersInfo(ctx context.Context) (map[string]*commonpb.LedgerInfo, error) {
	resp, err := g.client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
	if err != nil {
		return nil, err
	}

	return resp.Ledgers, nil
}

func (g *LedgerGrpcClient) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	return g.client.GetLedgerByName(ctx, &servicepb.GetLedgerByNameRequest{Name: name})
}

var _ Controller = (*LedgerGrpcClient)(nil)
