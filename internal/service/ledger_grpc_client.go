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

// CreateTransaction forwards the request via gRPC to the leader
func (g *LedgerGrpcClient) CreateTransaction(ctx context.Context, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	// Call leader via gRPC
	log, err := g.client.CreateTransaction(ctx, &ledgerpb.CreateTransactionRequest{
		Parameters: &ledgerpb.Parameters{
			Ledger:         g.name,
			IdempotencyKey: parameters.IdempotencyKey,
		},
		Payload: parameters.Input,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) RevertTransaction(ctx context.Context, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) SaveTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, err := g.client.SaveTransactionMetadata(ctx, &ledgerpb.SaveTransactionMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			Ledger:         g.name,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) SaveAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	// Call leader via gRPC
	log, err := g.client.SaveAccountMetadata(ctx, &ledgerpb.SaveAccountMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			Ledger:         g.name,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) DeleteAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) Import(ctx context.Context, stream chan *ledgerpb.Log) error {
	return ErrNotFound
}

func (g *LedgerGrpcClient) Export(ctx context.Context, w ExportWriter) error {
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

// GetLogByID retrieves a log by its ID (implements LogReader)
func (g *LedgerGrpcClient) GetLogByID(ctx context.Context, id uint64) (*ledgerpb.Log, error) {
	if id == 0 {
		return nil, nil
	}
	cursor, err := g.GetAllLogs(ctx, id-1, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cursor.Close()
	}()
	return cursor.Next(ctx)
}
