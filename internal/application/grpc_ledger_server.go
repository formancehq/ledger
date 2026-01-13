package application

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/grpc"
)

type LedgerServiceServerImpl struct {
	ledgerpb.UnimplementedLedgerServiceServer
	logger     logging.Logger
	systemNode *raft.Node
}

func NewLedgerServiceServer(logger logging.Logger, systemNode *raft.Node) ledgerpb.LedgerServiceServer {
	return &LedgerServiceServerImpl{
		logger:     logger,
		systemNode: systemNode,
	}
}

func (impl *LedgerServiceServerImpl) CreateTransaction(ctx context.Context, req *ledgerpb.CreateTransactionRequest) (*ledgerpb.Log, error) {
	impl.logger.WithFields(map[string]any{"reference": req.Payload.Reference}).Debugf("CreateTransaction request received")

	return impl.systemNode.CreateTransaction(ctx, req.Parameters.Ledger, service.Parameters[*ledgerpb.CreateTransactionRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) StreamLogs(req *ledgerpb.StreamLogsRequest, stream ledgerpb.LedgerService_StreamLogsServer) error {
	ctx := stream.Context()

	cursor, err := impl.systemNode.GetAllLogs(ctx, req.Ledger, req.FromId, req.ToId)
	if err != nil {
		return err
	}
	defer func() {
		_ = cursor.Close()
	}()

	for {
		log, err := cursor.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading log from store: %w", err)
		}

		// log is already *ledgerpb.Log
		if err := stream.Send(&ledgerpb.StreamLogsResponse{
			Log: log,
		}); err != nil {
			return fmt.Errorf("sending log: %w", err)
		}
	}

	return nil
}

func (impl *LedgerServiceServerImpl) SaveAccountMetadata(ctx context.Context, req *ledgerpb.SaveAccountMetadataRequest) (*ledgerpb.Log, error) {
	return impl.systemNode.SaveAccountMetadata(ctx, req.Parameters.Ledger, service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) SaveTransactionMetadata(ctx context.Context, req *ledgerpb.SaveTransactionMetadataRequest) (*ledgerpb.Log, error) {
	return impl.systemNode.SaveTransactionMetadata(ctx, req.Parameters.Ledger, service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) DeleteAccountMetadata(ctx context.Context, req *ledgerpb.DeleteAccountMetadataRequest) (*ledgerpb.Log, error) {
	return impl.systemNode.DeleteAccountMetadata(ctx, req.Parameters.Ledger, service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) DeleteTransactionMetadata(ctx context.Context, req *ledgerpb.DeleteTransactionMetadataRequest) (*ledgerpb.Log, error) {
	return impl.systemNode.DeleteTransactionMetadata(ctx, req.Parameters.Ledger, service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) RevertTransaction(ctx context.Context, req *ledgerpb.RevertTransactionRequest) (*ledgerpb.Log, error) {
	return impl.systemNode.RevertTransaction(ctx, req.Parameters.Ledger, service.Parameters[*ledgerpb.RevertTransactionRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) CreateLedger(ctx context.Context, req *ledgerpb.CreateLedgerCommand) (*ledgerpb.LedgerInfo, error) {
	return impl.systemNode.CreateLedger(ctx, req)
}

func (impl *LedgerServiceServerImpl) DeleteLedger(ctx context.Context, req *ledgerpb.DeleteLedgerCommand) (*ledgerpb.DeleteLedgerResponse, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("DeleteLedger request received")

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	if err := impl.systemNode.DeleteLedger(ctx, req.Name); err != nil {
		return nil, fmt.Errorf("deleting ledger: %w", err)
	}

	return &ledgerpb.DeleteLedgerResponse{}, nil
}

func (impl *LedgerServiceServerImpl) GetAllLedgersInfo(ctx context.Context, _ *ledgerpb.GetAllLedgersRequest) (*ledgerpb.GetAllLedgersResponse, error) {
	impl.logger.Debugf("GetAllLedgersInfo request received")

	ledgers, err := impl.systemNode.GetAllLedgersInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all ledgers: %w", err)
	}

	return &ledgerpb.GetAllLedgersResponse{
		Ledgers: ledgers,
	}, nil
}

func (impl *LedgerServiceServerImpl) GetLedgerInfo(ctx context.Context, req *ledgerpb.GetLedgerByNameRequest) (*ledgerpb.LedgerInfo, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("GetLedgerInfo request received")

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.systemNode.GetLedgerInfo(ctx, req.Name)
}

func RegisterLedgerService(server *grpc.Server, ledgerServiceServer ledgerpb.LedgerServiceServer) {
	ledgerpb.RegisterLedgerServiceServer(server, ledgerServiceServer)
}
