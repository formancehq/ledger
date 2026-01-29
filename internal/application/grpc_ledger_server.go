package application

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/grpc"
)

type LedgerServiceServerImpl struct {
	servicepb.UnimplementedLedgerServiceServer
	logger logging.Logger
	ctrl   service.Controller
}

func NewLedgerServiceServer(logger logging.Logger, ctrl service.Controller) servicepb.LedgerServiceServer {
	return &LedgerServiceServerImpl{
		logger: logger,
		ctrl:   ctrl,
	}
}

func (impl *LedgerServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*commonpb.Log, error) {
	if req.Action == nil {
		return nil, fmt.Errorf("action is required")
	}

	impl.logger.Debugf("Apply request received")
	return impl.ctrl.Apply(ctx, req.Action)
}

func (impl *LedgerServiceServerImpl) StreamLedgerLogs(req *servicepb.StreamLedgerLogsRequest, stream servicepb.LedgerService_StreamLedgerLogsServer) error {
	ctx := stream.Context()

	cursor, err := impl.ctrl.GetAllLedgerLogs(ctx, req.LedgerId, req.FromId, req.ToId)
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

		if err := stream.Send(&servicepb.StreamLedgerLogsResponse{
			Log: log,
		}); err != nil {
			return fmt.Errorf("sending log: %w", err)
		}
	}

	return nil
}

func (impl *LedgerServiceServerImpl) StreamLogs(req *servicepb.StreamLogsRequest, stream servicepb.LedgerService_StreamLogsServer) error {
	ctx := stream.Context()

	cursor, err := impl.ctrl.GetAllLogs(ctx, req.FromSequence, req.ToSequence)
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

		if err := stream.Send(&servicepb.StreamLogsResponse{
			Log: log,
		}); err != nil {
			return fmt.Errorf("sending log: %w", err)
		}
	}

	return nil
}

func (impl *LedgerServiceServerImpl) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*commonpb.Transaction, error) {
	return impl.ctrl.GetTransaction(ctx, req.LedgerId, req.TransactionId)
}

func (impl *LedgerServiceServerImpl) GetAllLedgersInfo(ctx context.Context, _ *servicepb.GetAllLedgersRequest) (*servicepb.GetAllLedgersResponse, error) {
	impl.logger.Debugf("GetAllLedgersInfo request received")

	ledgers, err := impl.ctrl.GetAllLedgersInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all ledgers: %w", err)
	}

	return &servicepb.GetAllLedgersResponse{
		Ledgers: ledgers,
	}, nil
}

func (impl *LedgerServiceServerImpl) GetLedgerByName(ctx context.Context, req *servicepb.GetLedgerByNameRequest) (*commonpb.LedgerInfo, error) {
	return impl.ctrl.GetLedgerByName(ctx, req.Name)
}

func RegisterLedgerService(server *grpc.Server, ledgerServiceServer servicepb.LedgerServiceServer) {
	servicepb.RegisterLedgerServiceServer(server, ledgerServiceServer)
}
