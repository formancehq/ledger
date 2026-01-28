package service

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftpb"
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
func (g *LedgerGrpcClient) CreateTransaction(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*commonpb.Log, error) {
	// Call leader via gRPC
	log, err := g.client.CreateTransaction(ctx, &ledgerpb.CreateTransactionRequest{
		Parameters: &ledgerpb.Parameters{
			LedgerId:       ledgerID,
			IdempotencyKey: parameters.IdempotencyKey,
		},
		Payload: parameters.Input,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) GetTransaction(ctx context.Context, ledgerID uint32, transactionID uint64) (*commonpb.Transaction, error) {
	return g.client.GetTransaction(ctx, &ledgerpb.GetTransactionRequest{
		LedgerId:      ledgerID,
		TransactionId: transactionID,
	})
}

func (g *LedgerGrpcClient) RevertTransaction(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*commonpb.Log, error) {
	return g.client.RevertTransaction(ctx, &ledgerpb.RevertTransactionRequest{
		Parameters: &ledgerpb.Parameters{
			LedgerId:       ledgerID,
			IdempotencyKey: parameters.IdempotencyKey,
		},
		Payload: parameters.Input,
	})
}

func (g *LedgerGrpcClient) SaveTransactionMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*commonpb.Log, error) {
	log, err := g.client.SaveTransactionMetadata(ctx, &ledgerpb.SaveTransactionMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			LedgerId:       ledgerID,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) SaveAccountMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*commonpb.Log, error) {
	// Call leader via gRPC
	log, err := g.client.SaveAccountMetadata(ctx, &ledgerpb.SaveAccountMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			LedgerId:       ledgerID,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) DeleteTransactionMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*commonpb.Log, error) {
	log, err := g.client.DeleteTransactionMetadata(ctx, &ledgerpb.DeleteTransactionMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			LedgerId:       ledgerID,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) DeleteAccountMetadata(ctx context.Context, ledgerID uint32, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*commonpb.Log, error) {
	log, err := g.client.DeleteAccountMetadata(ctx, &ledgerpb.DeleteAccountMetadataRequest{
		Payload: parameters.Input,
		Parameters: &ledgerpb.Parameters{
			LedgerId:       ledgerID,
			IdempotencyKey: parameters.IdempotencyKey,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return log, nil
}

func (g *LedgerGrpcClient) Import(ctx context.Context, ledgerID uint32, stream chan *commonpb.Log) error {
	return fmt.Errorf("import is not implemented yet")
}

func (g *LedgerGrpcClient) Export(ctx context.Context, ledgerID uint32, w ExportWriter) error {
	return fmt.Errorf("export is not implemented yet")
}

// GetAllLogs returns a cursor to iterate over all logs (implements LogReader)
func (g *LedgerGrpcClient) GetAllLogs(ctx context.Context, ledgerID uint32, from uint64, to uint64) (store.Cursor[*commonpb.Log], error) {
	req := &ledgerpb.StreamLogsRequest{
		LedgerId: ledgerID,
		FromId:   from,
		ToId:     to, // 0 means no limit
	}

	stream, err := g.client.StreamLogs(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Canceled {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	return store.NewGRPCStreamCursor(stream, func(res *ledgerpb.StreamLogsResponse) (*commonpb.Log, error) {
		return res.Log, nil
	}), nil
}

// GetLogByID retrieves a log by its ID (implements LogReader)
func (g *LedgerGrpcClient) GetLogByID(ctx context.Context, ledgerID uint32, id uint64) (*commonpb.Log, error) {
	if id == 0 {
		return nil, nil
	}
	cursor, err := g.GetAllLogs(ctx, ledgerID, id-1, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = cursor.Close()
	}()
	return cursor.Next(ctx)
}

func (g *LedgerGrpcClient) CreateLedger(ctx context.Context, request *raftpb.CreateLedgerCommand) (*commonpb.LedgerInfo, error) {
	return g.client.CreateLedger(ctx, &ledgerpb.CreateLedgerRequest{
		Name:     request.Name,
		Metadata: request.Metadata,
	})
}

func (g *LedgerGrpcClient) DeleteLedger(ctx context.Context, id uint32) error {
	_, err := g.client.DeleteLedger(ctx, &ledgerpb.DeleteLedgerRequest{Id: id})
	return err
}

func (g *LedgerGrpcClient) GetAllLedgersInfo(ctx context.Context) (map[string]*commonpb.LedgerInfo, error) {
	resp, err := g.client.GetAllLedgersInfo(ctx, &ledgerpb.GetAllLedgersRequest{})
	if err != nil {
		return nil, err
	}

	return resp.Ledgers, nil
}

func (g *LedgerGrpcClient) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	return g.client.GetLedgerByName(ctx, &ledgerpb.GetLedgerByNameRequest{Name: name})
}
