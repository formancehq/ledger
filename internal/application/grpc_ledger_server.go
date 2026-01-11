package application

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/grpc"
)

type LedgerServiceServerImpl struct {
	ledgerpb.UnimplementedLedgerServiceServer
	logger     logging.Logger
	systemNode *system.Node
}

func NewLedgerServiceServer(logger logging.Logger, systemNode *system.Node) ledgerpb.LedgerServiceServer {
	return &LedgerServiceServerImpl{
		logger:     logger,
		systemNode: systemNode,
	}
}

func (impl *LedgerServiceServerImpl) CreateTransaction(ctx context.Context, req *ledgerpb.CreateTransactionRequest) (*ledgerpb.Log, error) {
	impl.logger.WithFields(map[string]any{"reference": req.Payload.Reference}).Debugf("CreateTransaction request received")

	// Create transaction parameters directly from protobuf request
	params := service.Parameters[*ledgerpb.CreateTransactionRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	}

	// Extract ledger name from request
	ledgerName := req.Parameters.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", ledgerName, err)
	}

	// Call ledger service
	log, err := ledgerNode.CreateTransaction(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("creating transaction: %w", err)
	}

	return log, nil
}

func (impl *LedgerServiceServerImpl) StreamLogs(req *ledgerpb.StreamLogsRequest, stream ledgerpb.LedgerService_StreamLogsServer) error {
	ctx := stream.Context()

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, req.GetLedger())
	if err != nil {
		return err
	}

	cursor, err := ledgerNode.GetAllLogs(ctx, req.FromId, req.ToId)
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
	impl.logger.WithFields(map[string]any{"address": req.Payload.Address}).Debugf("SaveAccountMetadata request received")

	// Validate request
	if req.Payload.Address == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if req.Payload.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	// Extract ledger name from request
	ledgerName := req.Parameters.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", ledgerName, err)
	}

	// Create parameters directly from protobuf request
	params := service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	}

	// Call ledger service
	log, err := ledgerNode.SaveAccountMetadata(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("saving account metadata: %w", err)
	}

	return log, nil
}

func (impl *LedgerServiceServerImpl) SaveTransactionMetadata(ctx context.Context, req *ledgerpb.SaveTransactionMetadataRequest) (*ledgerpb.Log, error) {
	impl.logger.WithFields(map[string]any{"transaction_id": req.Payload.TransactionId}).Debugf("SaveTransactionMetadata request received")

	// Validate request
	if req.Payload.TransactionId == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}
	if req.Payload.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	// Extract ledger name from request
	ledgerName := req.Parameters.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", ledgerName, err)
	}

	// Create parameters directly from protobuf request
	params := service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	}

	// Call ledger service
	log, err := ledgerNode.SaveTransactionMetadata(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("saving transaction metadata: %w", err)
	}

	return log, nil
}

func (impl *LedgerServiceServerImpl) DeleteAccountMetadata(ctx context.Context, req *ledgerpb.DeleteAccountMetadataRequest) (*ledgerpb.Log, error) {
	impl.logger.WithFields(map[string]any{"address": req.Payload.Address}).Debugf("DeleteAccountMetadata request received")

	if req.Payload.Address == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if req.Payload.Key == "" {
		return nil, fmt.Errorf("metadata key is required")
	}

	ledgerName := req.Parameters.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", ledgerName, err)
	}

	params := service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	}

	log, err := ledgerNode.DeleteAccountMetadata(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("deleting account metadata: %w", err)
	}

	return log, nil
}

func (impl *LedgerServiceServerImpl) DeleteTransactionMetadata(ctx context.Context, req *ledgerpb.DeleteTransactionMetadataRequest) (*ledgerpb.Log, error) {
	impl.logger.WithFields(map[string]any{"transaction_id": req.Payload.TransactionId}).Debugf("DeleteTransactionMetadata request received")

	if req.Payload.TransactionId == 0 {
		return nil, fmt.Errorf("transaction id is required")
	}
	if req.Payload.Key == "" {
		return nil, fmt.Errorf("metadata key is required")
	}

	ledgerName := req.Parameters.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", ledgerName, err)
	}

	params := service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	}

	log, err := ledgerNode.DeleteTransactionMetadata(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("deleting transaction metadata: %w", err)
	}

	return log, nil
}

func RegisterLedgerService(server *grpc.Server, ledgerServiceServer ledgerpb.LedgerServiceServer) {
	ledgerpb.RegisterLedgerServiceServer(server, ledgerServiceServer)
}
