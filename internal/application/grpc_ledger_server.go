package application

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/grpc"
)

type LedgerServiceServerImpl struct {
	ledgerpb.UnimplementedLedgerServiceServer
	logger logging.Logger
	ctrl   service.Controller
}

func NewLedgerServiceServer(logger logging.Logger, ctrl service.Controller) ledgerpb.LedgerServiceServer {
	return &LedgerServiceServerImpl{
		logger: logger,
		ctrl:   ctrl,
	}
}

func (impl *LedgerServiceServerImpl) CreateTransaction(ctx context.Context, req *ledgerpb.CreateTransactionRequest) (*commonpb.Log, error) {
	impl.logger.WithFields(map[string]any{"reference": req.Payload.Reference}).Debugf("CreateTransaction request received")

	return impl.ctrl.CreateTransaction(ctx, req.Parameters.LedgerId, service.Parameters[*ledgerpb.CreateTransactionRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) StreamLogs(req *ledgerpb.StreamLogsRequest, stream ledgerpb.LedgerService_StreamLogsServer) error {
	ctx := stream.Context()

	cursor, err := impl.ctrl.GetAllLogs(ctx, req.LedgerId, req.FromId, req.ToId)
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

func (impl *LedgerServiceServerImpl) SaveAccountMetadata(ctx context.Context, req *ledgerpb.SaveAccountMetadataRequest) (*commonpb.Log, error) {
	return impl.ctrl.SaveAccountMetadata(ctx, req.Parameters.LedgerId, service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) SaveTransactionMetadata(ctx context.Context, req *ledgerpb.SaveTransactionMetadataRequest) (*commonpb.Log, error) {
	return impl.ctrl.SaveTransactionMetadata(ctx, req.Parameters.LedgerId, service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) DeleteAccountMetadata(ctx context.Context, req *ledgerpb.DeleteAccountMetadataRequest) (*commonpb.Log, error) {
	return impl.ctrl.DeleteAccountMetadata(ctx, req.Parameters.LedgerId, service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) DeleteTransactionMetadata(ctx context.Context, req *ledgerpb.DeleteTransactionMetadataRequest) (*commonpb.Log, error) {
	return impl.ctrl.DeleteTransactionMetadata(ctx, req.Parameters.LedgerId, service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) RevertTransaction(ctx context.Context, req *ledgerpb.RevertTransactionRequest) (*commonpb.Log, error) {
	return impl.ctrl.RevertTransaction(ctx, req.Parameters.LedgerId, service.Parameters[*ledgerpb.RevertTransactionRequestPayload]{
		IdempotencyKey: req.Parameters.IdempotencyKey,
		Input:          req.Payload,
	})
}

func (impl *LedgerServiceServerImpl) GetTransaction(ctx context.Context, req *ledgerpb.GetTransactionRequest) (*commonpb.Transaction, error) {
	return impl.ctrl.GetTransaction(ctx, req.LedgerId, req.TransactionId)
}

func (impl *LedgerServiceServerImpl) CreateLedger(ctx context.Context, req *ledgerpb.CreateLedgerRequest) (*commonpb.LedgerInfo, error) {
	return impl.ctrl.CreateLedger(ctx, &raftpb.CreateLedgerCommand{
		Name:     req.Name,
		Metadata: req.Metadata,
	})
}

func (impl *LedgerServiceServerImpl) DeleteLedger(ctx context.Context, req *ledgerpb.DeleteLedgerRequest) (*ledgerpb.DeleteLedgerResponse, error) {
	impl.logger.WithFields(map[string]any{"id": req.Id}).Debugf("DeleteLedger request received")

	if err := impl.ctrl.DeleteLedger(ctx, req.Id); err != nil {
		return nil, fmt.Errorf("deleting ledger: %w", err)
	}

	return &ledgerpb.DeleteLedgerResponse{}, nil
}

func (impl *LedgerServiceServerImpl) GetAllLedgersInfo(ctx context.Context, _ *ledgerpb.GetAllLedgersRequest) (*ledgerpb.GetAllLedgersResponse, error) {
	impl.logger.Debugf("GetAllLedgersInfo request received")

	ledgers, err := impl.ctrl.GetAllLedgersInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all ledgers: %w", err)
	}

	return &ledgerpb.GetAllLedgersResponse{
		Ledgers: ledgers,
	}, nil
}

func (impl *LedgerServiceServerImpl) GetLedgerByName(ctx context.Context, req *ledgerpb.GetLedgerByNameRequest) (*commonpb.LedgerInfo, error) {
	return impl.ctrl.GetLedgerByName(ctx, req.Name)
}

func RegisterLedgerService(server *grpc.Server, ledgerServiceServer ledgerpb.LedgerServiceServer) {
	ledgerpb.RegisterLedgerServiceServer(server, ledgerServiceServer)
}
