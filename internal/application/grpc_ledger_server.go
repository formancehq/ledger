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
	"github.com/formancehq/ledger-v3-poc/internal/service/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"google.golang.org/grpc"
)

type BucketServiceServerImpl struct {
	servicepb.UnimplementedBucketServiceServer
	logger        logging.Logger
	ctrl          ctrl.Controller
	store         *data.Store
	attrs         *attributes.Attributes
	auditEnabled  bool
	receiptSigner *receipt.Signer
}

func NewBucketServiceServer(logger logging.Logger, ctrl ctrl.Controller, s *data.Store, attrs *attributes.Attributes, auditEnabled bool, receiptSigner *receipt.Signer) servicepb.BucketServiceServer {
	return &BucketServiceServerImpl{
		logger:        logger,
		ctrl:          ctrl,
		store:         s,
		attrs:         attrs,
		auditEnabled:  auditEnabled,
		receiptSigner: receiptSigner,
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

	// Sign receipts for created transactions (outside FSM to avoid Raft nondeterminism)
	if impl.receiptSigner != nil {
		for _, log := range logs {
			impl.signReceiptIfNeeded(log)
		}
	}

	return &servicepb.ApplyResponse{Logs: logs}, nil
}

// signReceiptIfNeeded signs a JWT receipt for logs containing created transactions.
func (impl *BucketServiceServerImpl) signReceiptIfNeeded(log *commonpb.Log) {
	applyLog := log.Payload.GetApply()
	if applyLog == nil || applyLog.Log == nil {
		return
	}
	created := applyLog.Log.Data.GetCreatedTransaction()
	if created == nil || created.Transaction == nil {
		return
	}

	tx := created.Transaction
	receiptToken, err := impl.receiptSigner.Sign(
		applyLog.LedgerName,
		tx.Id,
		tx.Postings,
		tx.Timestamp,
		0, // period ID is not tracked on individual logs
	)
	if err != nil {
		impl.logger.Errorf("Failed to sign receipt for tx %d: %v", tx.Id, err)
		return
	}
	log.Receipt = receiptToken
}

func (impl *BucketServiceServerImpl) ListPeriods(_ *servicepb.ListPeriodsRequest, stream servicepb.BucketService_ListPeriodsServer) error {
	periods, err := impl.ctrl.ListPeriods(stream.Context())
	if err != nil {
		return fmt.Errorf("listing periods: %w", err)
	}
	for _, period := range periods {
		if err := stream.Send(period); err != nil {
			return fmt.Errorf("sending period: %w", err)
		}
	}
	return nil
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

func (impl *BucketServiceServerImpl) ListAccounts(req *servicepb.ListAccountsRequest, stream servicepb.BucketService_ListAccountsServer) error {
	if req.Ledger == "" {
		return fmt.Errorf("ledger name is required")
	}

	impl.logger.Debugf("ListAccounts request received for ledger %s (pageSize=%d, afterAddress=%q, prefix=%q)",
		req.Ledger, req.PageSize, req.AfterAddress, req.Prefix)

	ctx := stream.Context()
	cursor, err := impl.ctrl.ListAccounts(ctx, req.Ledger, req.PageSize, req.AfterAddress, req.Prefix)
	if err != nil {
		return fmt.Errorf("listing accounts: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	for {
		account, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading account: %w", err)
		}
		if err := stream.Send(account); err != nil {
			return fmt.Errorf("sending account: %w", err)
		}
	}

	return nil
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

	cursor, err := impl.store.ListAuditEntries(req.AfterSequence) //nolint:protogetter
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

func (impl *BucketServiceServerImpl) GetEventsSinks(_ context.Context, _ *servicepb.GetEventsSinksRequest) (*servicepb.GetEventsSinksResponse, error) {
	sinks, err := impl.store.LoadAllSinkConfigs()
	if err != nil {
		return nil, fmt.Errorf("loading sink configs: %w", err)
	}

	statuses, err := impl.store.LoadAllSinkStatuses()
	if err != nil {
		return nil, fmt.Errorf("loading sink statuses: %w", err)
	}

	return &servicepb.GetEventsSinksResponse{
		Sinks:        sinks,
		SinkStatuses: statuses,
	}, nil
}

func RegisterBucketService(server *grpc.Server, ledgerServiceServer servicepb.BucketServiceServer) {
	servicepb.RegisterBucketServiceServer(server, ledgerServiceServer)
}
