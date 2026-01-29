package application

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/grpc"
)

type LedgerServiceServerImpl struct {
	servicepb.UnimplementedLedgerServiceServer
	logger logging.Logger
	ctrl   service.Controller
	store  store.Store
	node   *raft.Node
}

func NewLedgerServiceServer(logger logging.Logger, ctrl service.Controller, s store.Store, node *raft.Node) servicepb.LedgerServiceServer {
	return &LedgerServiceServerImpl{
		logger: logger,
		ctrl:   ctrl,
		store:  s,
		node:   node,
	}
}

func (impl *LedgerServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	if len(req.Actions) == 0 {
		return nil, fmt.Errorf("at least one action is required")
	}

	impl.logger.Debugf("Apply request received with %d actions", len(req.Actions))

	logs, err := impl.ctrl.Apply(ctx, req.Actions...)
	if err != nil {
		return nil, err
	}

	return &servicepb.ApplyResponse{Logs: logs}, nil
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
	// Get ledger ID from name or ID
	var ledgerID uint32
	if req.Ledger.GetName() != "" {
		ledger, err := impl.ctrl.GetLedgerByName(ctx, req.Ledger.GetName())
		if err != nil {
			return nil, fmt.Errorf("getting ledger: %w", err)
		}
		ledgerID = ledger.Id
	} else {
		ledgerID = req.Ledger.GetId()
	}

	return impl.ctrl.GetTransaction(ctx, ledgerID, req.TransactionId)
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

func (impl *LedgerServiceServerImpl) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	if req.Ledger.GetName() != "" {
		return impl.ctrl.GetLedgerByName(ctx, req.Ledger.GetName())
	}
	return impl.store.GetLedgerByID(ctx, req.Ledger.GetId())
}

func (impl *LedgerServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	// Get ledger ID from name or ID
	var ledgerID uint32
	if req.Ledger.GetName() != "" {
		ledger, err := impl.ctrl.GetLedgerByName(ctx, req.Ledger.GetName())
		if err != nil {
			return nil, fmt.Errorf("getting ledger: %w", err)
		}
		ledgerID = ledger.Id
	} else {
		ledgerID = req.Ledger.GetId()
	}

	return impl.ctrl.GetAccount(ctx, ledgerID, req.Address)
}

func (impl *LedgerServiceServerImpl) GetStoreMetrics(ctx context.Context, _ *servicepb.GetStoreMetricsRequest) (*servicepb.GetStoreMetricsResponse, error) {
	// Check if the store supports metrics (only Pebble does)
	metricsProvider, ok := impl.store.(store.MetricsProvider)
	if !ok {
		return &servicepb.GetStoreMetricsResponse{
			Available: false,
		}, nil
	}

	// Get metrics from the store (already in proto format)
	metrics, ok := metricsProvider.GetMetrics().(*servicepb.PebbleMetrics)
	if !ok {
		return &servicepb.GetStoreMetricsResponse{
			Available: false,
		}, nil
	}

	return &servicepb.GetStoreMetricsResponse{
		Available: true,
		Metrics:   metrics,
	}, nil
}

func (impl *LedgerServiceServerImpl) GetClusterState(ctx context.Context, _ *servicepb.GetClusterStateRequest) (*raftcmdpb.ClusterState, error) {
	return impl.node.GetClusterState(ctx)
}

func RegisterLedgerService(server *grpc.Server, ledgerServiceServer servicepb.LedgerServiceServer) {
	servicepb.RegisterLedgerServiceServer(server, ledgerServiceServer)
}
