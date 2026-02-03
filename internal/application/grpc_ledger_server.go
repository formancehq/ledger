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
	store  *store.Store
	node   *raft.Node
}

func NewLedgerServiceServer(logger logging.Logger, ctrl service.Controller, s *store.Store, node *raft.Node) servicepb.LedgerServiceServer {
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
		log, err := cursor.Next()
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
	// Get ledger name from request
	var ledgerName string
	if req.Ledger.GetName() != "" {
		ledgerName = req.Ledger.GetName()
	} else {
		// Resolve ledger name from ID
		ledger, err := impl.store.GetLedgerByID(req.Ledger.GetId())
		if err != nil {
			return nil, fmt.Errorf("getting ledger: %w", err)
		}
		ledgerName = ledger.Name
	}

	return impl.ctrl.GetTransaction(ctx, ledgerName, req.TransactionId)
}

func (impl *LedgerServiceServerImpl) GetAllLedgersInfo(_ *servicepb.GetAllLedgersRequest, stream servicepb.LedgerService_GetAllLedgersInfoServer) error {
	impl.logger.Debugf("GetAllLedgersInfo request received")

	ctx := stream.Context()
	cursor, err := impl.ctrl.GetAllLedgersInfo(ctx)
	if err != nil {
		return fmt.Errorf("getting all ledgers: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	for {
		ledger, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading ledger: %w", err)
		}
		if err := stream.Send(ledger); err != nil {
			return fmt.Errorf("sending ledger: %w", err)
		}
	}

	return nil
}

func (impl *LedgerServiceServerImpl) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	if req.Ledger.GetName() != "" {
		return impl.ctrl.GetLedgerByName(ctx, req.Ledger.GetName())
	}
	return impl.store.GetLedgerByID(req.Ledger.GetId())
}

func (impl *LedgerServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	// Get ledger name from request
	var ledgerName string
	if req.Ledger.GetName() != "" {
		ledgerName = req.Ledger.GetName()
	} else {
		// Resolve ledger name from ID
		ledger, err := impl.store.GetLedgerByID(req.Ledger.GetId())
		if err != nil {
			return nil, fmt.Errorf("getting ledger: %w", err)
		}
		ledgerName = ledger.Name
	}

	return impl.ctrl.GetAccount(ctx, ledgerName, req.Address)
}

func (impl *LedgerServiceServerImpl) GetStoreMetrics(ctx context.Context, _ *servicepb.GetStoreMetricsRequest) (*servicepb.GetStoreMetricsResponse, error) {
	// Get metrics from the Pebble store directly
	metrics, ok := impl.store.GetMetrics().(*servicepb.PebbleMetrics)
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
