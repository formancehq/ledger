package application

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"google.golang.org/grpc"
)

type BucketServiceServerImpl struct {
	servicepb.UnimplementedBucketServiceServer
	logger logging.Logger
	ctrl   ctrl.Controller
	store  *data.Store
}

func NewBucketServiceServer(logger logging.Logger, ctrl ctrl.Controller, s *data.Store) servicepb.BucketServiceServer {
	return &BucketServiceServerImpl{
		logger: logger,
		ctrl:   ctrl,
		store:  s,
	}
}

func (impl *BucketServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	if len(req.Requests) == 0 {
		return nil, fmt.Errorf("at least one request is required")
	}

	impl.logger.Debugf("Apply request received with %d requests", len(req.Requests))

	logs, err := impl.ctrl.Apply(ctx, req.Requests...)
	if err != nil {
		return nil, err
	}

	return &servicepb.ApplyResponse{Logs: logs}, nil
}

func (impl *BucketServiceServerImpl) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*commonpb.Transaction, error) {
	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.ctrl.GetTransaction(ctx, req.Ledger, req.TransactionId)
}

func (impl *BucketServiceServerImpl) ListTransactions(req *servicepb.ListTransactionsRequest, stream servicepb.BucketService_ListTransactionsServer) error {
	if req.Ledger == "" {
		return fmt.Errorf("ledger name is required")
	}

	impl.logger.Debugf("ListTransactions request received for ledger %s (pageSize=%d, afterTxID=%d)",
		req.Ledger, req.PageSize, req.AfterTxId)

	ctx := stream.Context()
	cursor, err := impl.ctrl.ListTransactions(ctx, req.Ledger, req.PageSize, req.AfterTxId)
	if err != nil {
		return fmt.Errorf("listing transactions: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	for {
		tx, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading transaction: %w", err)
		}
		if err := stream.Send(tx); err != nil {
			return fmt.Errorf("sending transaction: %w", err)
		}
	}

	return nil
}

func (impl *BucketServiceServerImpl) GetAllLedgersInfo(_ *servicepb.GetAllLedgersRequest, stream servicepb.BucketService_GetAllLedgersInfoServer) error {
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

func (impl *BucketServiceServerImpl) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}
	return impl.ctrl.GetLedgerByName(ctx, req.Ledger)
}

func (impl *BucketServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.ctrl.GetAccount(ctx, req.Ledger, req.Address)
}

func (impl *BucketServiceServerImpl) GetStoreMetrics(_ context.Context, _ *servicepb.GetStoreMetricsRequest) (*servicepb.GetStoreMetricsResponse, error) {
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

func RegisterBucketService(server *grpc.Server, ledgerServiceServer servicepb.BucketServiceServer) {
	servicepb.RegisterBucketServiceServer(server, ledgerServiceServer)
}
