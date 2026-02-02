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
func (g *LedgerGrpcClient) Apply(ctx context.Context, actions ...*servicepb.Request) ([]*commonpb.Log, error) {
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
		Ledger: &servicepb.LedgerNameOrId{
			Type: &servicepb.LedgerNameOrId_Id{Id: ledgerID},
		},
		TransactionId: transactionID,
	})
}

func (g *LedgerGrpcClient) GetAccount(ctx context.Context, ledgerID uint32, address string) (*commonpb.Account, error) {
	// GetAccount reads from local store via RoutedController, this method should not be called
	return nil, fmt.Errorf("GetAccount is not available via gRPC client - use local reads")
}

func (g *LedgerGrpcClient) Import(ctx context.Context, ledgerID uint32, stream chan *commonpb.LedgerLog) error {
	return fmt.Errorf("import is not implemented yet")
}

func (g *LedgerGrpcClient) Export(ctx context.Context, ledgerID uint32, w ExportWriter) error {
	return fmt.Errorf("export is not implemented yet")
}

// GetAllLogs returns a cursor to iterate over all logs (implements Controller interface)
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

func (g *LedgerGrpcClient) GetAllLedgersInfo(ctx context.Context) (store.Cursor[*commonpb.LedgerInfo], error) {
	stream, err := g.client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
	if err != nil {
		return nil, err
	}

	return store.NewGRPCStreamCursor(stream, func(res *commonpb.LedgerInfo) (*commonpb.LedgerInfo, error) {
		return res, nil
	}), nil
}

func (g *LedgerGrpcClient) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	return g.client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: &servicepb.LedgerNameOrId{
			Type: &servicepb.LedgerNameOrId_Name{Name: name},
		},
	})
}

var _ Controller = (*LedgerGrpcClient)(nil)
