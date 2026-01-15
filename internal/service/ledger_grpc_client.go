package service

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LedgerGrpcClient implements Ledger by forwarding requests via gRPC to the leader
type LedgerGrpcClient struct {
	client ledgerpb.LedgerServiceClient
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation
func NewLedgerGrpcClient(client ledgerpb.LedgerServiceClient) *LedgerGrpcClient {
	return &LedgerGrpcClient{
		client: client,
	}
}

// CreateTransaction forwards the request via gRPC to the leader
func (g *LedgerGrpcClient) CreateTransaction(ctx context.Context, ledger string, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	// Call leader via gRPC
	log, err := g.client.CreateTransaction(ctx, &ledgerpb.CreateTransactionRequest{
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledger,
			IdempotencyKey: parameters.IdempotencyKey,
		},
		Payload: parameters.Input,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) RevertTransaction(ctx context.Context, ledger string, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return g.client.RevertTransaction(ctx, &ledgerpb.RevertTransactionRequest{
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledger,
			IdempotencyKey: parameters.IdempotencyKey,
		},
		Payload: parameters.Input,
	})
}

func (g *LedgerGrpcClient) SaveTransactionMetadata(ctx context.Context, ledger string, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, err := g.client.SaveTransactionMetadata(ctx, &ledgerpb.SaveTransactionMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledger,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) SaveAccountMetadata(ctx context.Context, ledger string, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	// Call leader via gRPC
	log, err := g.client.SaveAccountMetadata(ctx, &ledgerpb.SaveAccountMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledger,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) DeleteTransactionMetadata(ctx context.Context, ledger string, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, err := g.client.DeleteTransactionMetadata(ctx, &ledgerpb.DeleteTransactionMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledger,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) DeleteAccountMetadata(ctx context.Context, ledger string, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	log, err := g.client.DeleteAccountMetadata(ctx, &ledgerpb.DeleteAccountMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			Ledger:         ledger,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) Import(ctx context.Context, ledger string, stream chan *ledgerpb.Log) error {
	return fmt.Errorf("import is not implemented yet")
}

func (g *LedgerGrpcClient) Export(ctx context.Context, ledger string, w ExportWriter) error {
	return fmt.Errorf("export is not implemented yet")
}

// GetAllLogs returns a cursor to iterate over all logs (implements LogReader)
func (g *LedgerGrpcClient) GetAllLogs(ctx context.Context, ledger string, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	req := &ledgerpb.StreamLogsRequest{
		Ledger: ledger,
		FromId: from,
		ToId:   to, // 0 means no limit
	}

	stream, err := g.client.StreamLogs(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Canceled {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return store.NewGRPCStreamCursor(stream, func(res *ledgerpb.StreamLogsResponse) (*ledgerpb.Log, error) {
		return res.Log, nil
	}), nil
}

// GetLogByID retrieves a log by its ID (implements LogReader)
func (g *LedgerGrpcClient) GetLogByID(ctx context.Context, ledger string, id uint64) (*ledgerpb.Log, error) {
	if id == 0 {
		return nil, nil
	}
	cursor, err := g.GetAllLogs(ctx, ledger, id-1, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cursor.Close()
	}()
	return cursor.Next(ctx)
}

func (g *LedgerGrpcClient) CreateLedger(ctx context.Context, request *ledgerpb.CreateLedgerCommand) (*ledgerpb.LedgerInfo, error) {
	return g.client.CreateLedger(ctx, request)
}

func (g *LedgerGrpcClient) DeleteLedger(ctx context.Context, name string) error {
	_, err := g.client.DeleteLedger(ctx, &ledgerpb.DeleteLedgerCommand{Name: name})
	return err
}

func (g *LedgerGrpcClient) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	resp, err := g.client.GetAllLedgersInfo(ctx, &ledgerpb.GetAllLedgersRequest{})
	if err != nil {
		return nil, err
	}

	return resp.Ledgers, nil
}

func (g *LedgerGrpcClient) GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	return g.client.GetLedgerInfo(ctx, &ledgerpb.GetLedgerByNameRequest{Name: name})
}
