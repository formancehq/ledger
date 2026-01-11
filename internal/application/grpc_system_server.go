package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/systempb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SystemServiceServerImpl struct {
	systempb.UnimplementedSystemServiceServer
	logger     logging.Logger
	systemNode *system.Node
}

func NewSystemServiceServer(logger logging.Logger, cluster *system.Node) systempb.SystemServiceServer {
	return &SystemServiceServerImpl{
		logger: logger.WithFields(map[string]any{
			"service": "system.grpc-server",
		}),
		systemNode: cluster,
	}
}

func (impl *SystemServiceServerImpl) CreateLedger(ctx context.Context, req *systempb.CreateLedgerRequest) (*ledgerpb.LedgerInfo, error) {
	impl.logger.
		WithFields(map[string]any{"name": req.Name, "log_store_driver": req.LogStoreDriver, "runtime_store_driver": req.RuntimeStoreDriver}).
		Infof("CreateLedger request received")

	return impl.systemNode.CreateLedger(ctx, req)
}

func (impl *SystemServiceServerImpl) DeleteLedger(ctx context.Context, req *systempb.DeleteLedgerRequest) (*systempb.DeleteLedgerResponse, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("DeleteLedger request received")

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	if err := impl.systemNode.DeleteLedger(ctx, req.Name); err != nil {
		return nil, fmt.Errorf("deleting ledger: %w", err)
	}

	return &systempb.DeleteLedgerResponse{}, nil
}

func (impl *SystemServiceServerImpl) ResolveLedger(ctx context.Context, req *systempb.ResolveLedgerRequest) (*systempb.ResolveLedgerResponse, error) {
	impl.logger.
		WithFields(map[string]any{"ledger_name": req.LedgerName}).
		Debugf("ResolveLedger request received")

	if req.LedgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerName, ledgerID, err := impl.systemNode.ResolveLedger(ctx, req.LedgerName)
	if err != nil {
		if errors.Is(err, &ledgerpb.NotFoundError{}) {
			return nil, status.New(codes.NotFound, err.Error()).Err()
		}
		return nil, fmt.Errorf("resolving ledger '%s': %w", req.LedgerName, err)
	}

	return &systempb.ResolveLedgerResponse{
		LedgerName: ledgerName,
		LedgerId:   ledgerID,
	}, nil
}

func (impl *SystemServiceServerImpl) GetAllLedgersInfo(ctx context.Context, _ *systempb.GetAllLedgersRequest) (*systempb.GetAllLedgersResponse, error) {
	impl.logger.Debugf("GetAllLedgersInfo request received")

	ledgers, err := impl.systemNode.GetAllLedgersInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all ledgers: %w", err)
	}

	return &systempb.GetAllLedgersResponse{
		Ledgers: ledgers,
	}, nil
}

func (impl *SystemServiceServerImpl) GetLedgerInfo(ctx context.Context, req *systempb.GetLedgerByNameRequest) (*ledgerpb.LedgerInfo, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("GetLedgerInfo request received")

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.systemNode.GetLedgerInfo(ctx, req.Name)
}

func (impl *SystemServiceServerImpl) ResolveLedgerLeader(ctx context.Context, req *systempb.ResolveLedgerLeaderRequest) (*systempb.ResolveLedgerLeaderResponse, error) {
	impl.logger.WithFields(map[string]any{"ledger_name": req.LedgerName}).Debugf("ResolveLedgerLeader request received")

	if req.LedgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, req.LedgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", req.LedgerName, err)
	}

	return &systempb.ResolveLedgerLeaderResponse{
		LeaderId: ledgerNode.GetLeader(),
	}, nil
}

func RegisterSystemService(server *grpc.Server, systemServiceServer systempb.SystemServiceServer) {
	systempb.RegisterSystemServiceServer(server, systemServiceServer)
}
