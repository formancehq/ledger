package application

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/check"
	"github.com/formancehq/ledger-v3-poc/internal/service/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"google.golang.org/grpc"
)

type BucketServiceServerImpl struct {
	servicepb.UnimplementedBucketServiceServer
	logger       logging.Logger
	ctrl         ctrl.Controller
	store        *data.Store
	attrs        *attributes.Attributes
	auditEnabled bool
}

func NewBucketServiceServer(logger logging.Logger, ctrl ctrl.Controller, s *data.Store, attrs *attributes.Attributes, auditEnabled bool) servicepb.BucketServiceServer {
	return &BucketServiceServerImpl{
		logger:       logger,
		ctrl:         ctrl,
		store:        s,
		attrs:        attrs,
		auditEnabled: auditEnabled,
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

func (impl *BucketServiceServerImpl) CheckStore(_ *servicepb.CheckStoreRequest, stream servicepb.BucketService_CheckStoreServer) error {
	checker := check.NewChecker(impl.store, impl.attrs)
	return checker.Check(stream.Context(), func(event *servicepb.CheckStoreEvent) {
		_ = stream.Send(event)
	})
}

func (impl *BucketServiceServerImpl) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream servicepb.BucketService_ListAuditEntriesServer) error {
	if !impl.auditEnabled {
		return processing.ErrAuditDisabled
	}

	cursor, err := impl.store.ListAuditEntries(req.AfterSequence)
	if err != nil {
		return fmt.Errorf("listing audit entries: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	for {
		entry, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading audit entry: %w", err)
		}

		// Apply ledger filter: check if any order targets the requested ledger
		if req.Ledger != "" && !auditEntryMatchesLedger(entry, req.Ledger) {
			continue
		}

		// Apply failures-only filter
		if req.FailuresOnly && entry.GetFailure() == nil {
			continue
		}

		if err := stream.Send(entry); err != nil {
			return fmt.Errorf("sending audit entry: %w", err)
		}
	}

	return nil
}

// auditEntryMatchesLedger checks if any order in the audit entry targets the given ledger.
func auditEntryMatchesLedger(entry *auditpb.AuditEntry, ledger string) bool {
	for _, order := range entry.Orders {
		switch t := order.Type.(type) {
		case *raftcmdpb.Order_Apply:
			if t.Apply.Ledger == ledger {
				return true
			}
		case *raftcmdpb.Order_CreateLedger:
			if t.CreateLedger.Name == ledger {
				return true
			}
		case *raftcmdpb.Order_DeleteLedger:
			if t.DeleteLedger.Name == ledger {
				return true
			}
		}
	}
	return false
}

func RegisterBucketService(server *grpc.Server, ledgerServiceServer servicepb.BucketServiceServer) {
	servicepb.RegisterBucketServiceServer(server, ledgerServiceServer)
}
