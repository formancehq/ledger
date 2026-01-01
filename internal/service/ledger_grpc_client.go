package service

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// LedgerGrpcClient implements Ledger by forwarding requests via gRPC to the leader
type LedgerGrpcClient struct {
	client ledgerpb.LedgerServiceClient
	name   string
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation
func NewLedgerGrpcClient(name string, client ledgerpb.LedgerServiceClient) *LedgerGrpcClient {
	return &LedgerGrpcClient{
		client: client,
		name:   name,
	}
}

func (g *LedgerGrpcClient) Snapshot(ctx context.Context) error {
	_, err := g.client.Snapshot(ctx, &ledgerpb.LedgerSnapshotRequest{
		Ledger: g.name,
	})
	return err
}

// CreateTransaction forwards the request via gRPC to the leader
func (g *LedgerGrpcClient) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.CreatedTransaction, error) {
	// Call leader via gRPC
	resp, err := g.client.CreateTransaction(ctx, &ledgerpb.CreateTransactionRequest{
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledgerName,
			DryRun:         parameters.DryRun,
			IdempotencyKey: parameters.IdempotencyKey,
		},
		Payload: parameters.Input,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	// Response is already protobuf
	createdTx := &ledgerpb.CreatedTransaction{
		Transaction:     resp.Transaction,
		AccountMetadata: resp.AccountMetadata,
	}

	// Create log from CreatedTransaction (since CreateTransactionResponse doesn't include Log)
	logPayload := &ledgerpb.LogPayload{
		Payload: &ledgerpb.LogPayload_CreatedTransaction{
			CreatedTransaction: createdTx,
		},
	}
	log := &ledgerpb.Log{
		Id:   resp.Transaction.Id,
		Data: logPayload,
	}

	return log, createdTx, nil
}

func (g *LedgerGrpcClient) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

func (g *LedgerGrpcClient) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	// Call leader via gRPC
	resp, err := g.client.SaveAccountMetadata(ctx, &ledgerpb.SaveAccountMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledgerName,
			DryRun:         parameters.DryRun,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	// resp.Log is already *ledgerpb.Log
	return resp.Log, nil
}

func (g *LedgerGrpcClient) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) Import(ctx context.Context, ledgerName string, stream chan *ledgerpb.Log) error {
	return ErrNotFound
}

func (g *LedgerGrpcClient) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	return ErrNotFound
}

// GetAllLogs returns a cursor to iterate over all logs (implements LogReader)
func (g *LedgerGrpcClient) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error) {
	req := &ledgerpb.StreamLogsRequest{
		Ledger: g.name,
		FromId: from,
		ToId:   to, // 0 means no limit
	}

	stream, err := g.client.StreamLogs(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return NewGRPCStreamCursor(stream, func(res *ledgerpb.StreamLogsResponse) (*ledgerpb.Log, error) {
		return res.Log, nil
	}), nil
}
