package application

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

// LedgerGrpcClient implements Controller by forwarding requests via gRPC to the leader
type LedgerGrpcClient struct {
	client servicepb.BucketServiceClient
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation
func NewLedgerGrpcClient(client servicepb.BucketServiceClient) *LedgerGrpcClient {
	return &LedgerGrpcClient{
		client: client,
	}
}

// Apply forwards the requests via gRPC to the leader
func (g *LedgerGrpcClient) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	resp, err := g.client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: requests,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}
	return resp.Logs, nil
}

func (g *LedgerGrpcClient) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	return g.client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: transactionID,
	})
}

func (g *LedgerGrpcClient) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64) (data.Cursor[*commonpb.Transaction], error) {
	stream, err := g.client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:    ledgerName,
		PageSize:  pageSize,
		AfterTxId: afterTxID,
	})
	if err != nil {
		return nil, err
	}

	return data.NewGRPCStreamCursor(stream, func(res *commonpb.Transaction) (*commonpb.Transaction, error) {
		return res, nil
	}), nil
}

func (g *LedgerGrpcClient) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	// GetAccount reads from local store via RoutedController, this method should not be called
	return nil, fmt.Errorf("GetAccount is not available via gRPC client - use local reads")
}

func (g *LedgerGrpcClient) GetAllLedgersInfo(ctx context.Context) (data.Cursor[*commonpb.LedgerInfo], error) {
	stream, err := g.client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
	if err != nil {
		return nil, err
	}

	return data.NewGRPCStreamCursor(stream, func(res *commonpb.LedgerInfo) (*commonpb.LedgerInfo, error) {
		return res, nil
	}), nil
}

func (g *LedgerGrpcClient) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	return g.client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: name,
	})
}

var _ ctrl.Controller = (*LedgerGrpcClient)(nil)
