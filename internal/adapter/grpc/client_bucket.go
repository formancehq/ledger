package grpc

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// BucketGrpcClient implements Controller by forwarding requests via gRPC to the leader
type BucketGrpcClient struct {
	client servicepb.BucketServiceClient
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation
func NewLedgerGrpcClient(client servicepb.BucketServiceClient) *BucketGrpcClient {
	return &BucketGrpcClient{
		client: client,
	}
}

// Apply forwards the requests via gRPC to the leader
func (g *BucketGrpcClient) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	resp, err := g.client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: requests,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}
	return resp.Logs, nil
}

func (g *BucketGrpcClient) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	resp, err := g.client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: transactionID,
	})
	if err != nil {
		return nil, err
	}
	return resp.Transaction, nil
}

func (g *BucketGrpcClient) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64) (dal.Cursor[*commonpb.Transaction], error) {
	stream, err := g.client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:    ledgerName,
		PageSize:  pageSize,
		AfterTxId: afterTxID,
	})
	if err != nil {
		return nil, err
	}

	return dal.NewGRPCStreamCursor(stream, func(res *commonpb.Transaction) (*commonpb.Transaction, error) {
		return res, nil
	}), nil
}

func (g *BucketGrpcClient) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	// GetAccount reads from local store via RoutedController, this method should not be called
	return nil, fmt.Errorf("GetAccount is not available via gRPC client - use local reads")
}

func (g *BucketGrpcClient) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, prefix string) (dal.Cursor[*commonpb.Account], error) {
	stream, err := g.client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger:       ledgerName,
		PageSize:     pageSize,
		AfterAddress: afterAddress,
		Prefix:       prefix,
	})
	if err != nil {
		return nil, err
	}

	return dal.NewGRPCStreamCursor(stream, func(res *commonpb.Account) (*commonpb.Account, error) {
		return res, nil
	}), nil
}

func (g *BucketGrpcClient) ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32) (dal.Cursor[*commonpb.Log], error) {
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

	return dal.NewGRPCStreamCursor(stream, func(res *commonpb.Log) (*commonpb.Log, error) {
		return res, nil
	}), nil
}

func (g *BucketGrpcClient) ListLedgers(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error) {
	stream, err := g.client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}

	return dal.NewGRPCStreamCursor(stream, func(res *commonpb.LedgerInfo) (*commonpb.LedgerInfo, error) {
		return res, nil
	}), nil
}

func (g *BucketGrpcClient) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	return g.client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: name,
	})
}

func (g *BucketGrpcClient) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error) {
	req := &servicepb.ListAuditEntriesRequest{
		AfterSequence: afterSequence,
		FailuresOnly:  failuresOnly,
		PageSize:      pageSize,
	}
	stream, err := g.client.ListAuditEntries(ctx, req)
	if err != nil {
		return nil, err
	}

	return dal.NewGRPCStreamCursor(stream, func(res *auditpb.AuditEntry) (*auditpb.AuditEntry, error) {
		return res, nil
	}), nil
}

func (g *BucketGrpcClient) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	return g.client.GetLog(ctx, &servicepb.GetLogRequest{
		Sequence: sequence,
	})
}

func (g *BucketGrpcClient) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	return g.client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
		Sequence: sequence,
	})
}

func (g *BucketGrpcClient) ListPeriods(ctx context.Context) (dal.Cursor[*commonpb.Period], error) {
	stream, err := g.client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{})
	if err != nil {
		return nil, fmt.Errorf("gRPC ListPeriods call failed: %w", err)
	}

	return dal.NewGRPCStreamCursor(stream, func(res *commonpb.Period) (*commonpb.Period, error) {
		return res, nil
	}), nil
}

func (g *BucketGrpcClient) ListSigningKeys(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error) {
	stream, err := g.client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	if err != nil {
		return nil, fmt.Errorf("gRPC ListSigningKeys call failed: %w", err)
	}

	return dal.NewGRPCStreamCursor(stream, func(res *commonpb.SigningKey) (*commonpb.SigningKey, error) {
		return res, nil
	}), nil
}

func (g *BucketGrpcClient) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	return g.client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
		Ledger: ledgerName,
	})
}

func (g *BucketGrpcClient) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32) (*servicepb.AnalyzeAccountsResponse, error) {
	return g.client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
		Ledger:            ledgerName,
		VariableThreshold: variableThreshold,
	})
}

var _ ctrl.Controller = (*BucketGrpcClient)(nil)
