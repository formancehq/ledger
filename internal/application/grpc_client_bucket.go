package application

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/ctrl"
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
	resp, err := g.client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: transactionID,
	})
	if err != nil {
		return nil, err
	}
	return resp.Transaction, nil
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

func (g *LedgerGrpcClient) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, prefix string) (data.Cursor[*commonpb.Account], error) {
	stream, err := g.client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger:       ledgerName,
		PageSize:     pageSize,
		AfterAddress: afterAddress,
		Prefix:       prefix,
	})
	if err != nil {
		return nil, err
	}

	return data.NewGRPCStreamCursor(stream, func(res *commonpb.Account) (*commonpb.Account, error) {
		return res, nil
	}), nil
}

func (g *LedgerGrpcClient) ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32) (data.Cursor[*commonpb.Log], error) {
	req := &servicepb.ListLogsRequest{
		PageSize: pageSize,
	}
	if afterSequence > 0 {
		req.AfterSequence = &afterSequence
	}
	stream, err := g.client.ListLogs(ctx, req)
	if err != nil {
		return nil, err
	}

	return data.NewGRPCStreamCursor(stream, func(res *commonpb.Log) (*commonpb.Log, error) {
		return res, nil
	}), nil
}

func (g *LedgerGrpcClient) ListLedgers(ctx context.Context) (data.Cursor[*commonpb.LedgerInfo], error) {
	stream, err := g.client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
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

func (g *LedgerGrpcClient) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (data.Cursor[*auditpb.AuditEntry], error) {
	req := &servicepb.ListAuditEntriesRequest{
		AfterSequence: afterSequence,
		FailuresOnly:  failuresOnly,
		PageSize:      pageSize,
	}
	stream, err := g.client.ListAuditEntries(ctx, req)
	if err != nil {
		return nil, err
	}

	return data.NewGRPCStreamCursor(stream, func(res *auditpb.AuditEntry) (*auditpb.AuditEntry, error) {
		return res, nil
	}), nil
}

func (g *LedgerGrpcClient) ListPeriods(ctx context.Context) (data.Cursor[*commonpb.Period], error) {
	stream, err := g.client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{})
	if err != nil {
		return nil, fmt.Errorf("gRPC ListPeriods call failed: %w", err)
	}

	return data.NewGRPCStreamCursor(stream, func(res *commonpb.Period) (*commonpb.Period, error) {
		return res, nil
	}), nil
}

var _ ctrl.Controller = (*LedgerGrpcClient)(nil)
