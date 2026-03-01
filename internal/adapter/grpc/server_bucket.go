package grpc

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/application/check"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/application/events"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/infra/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	ggrpc "google.golang.org/grpc"
)

type BucketServiceServerImpl struct {
	servicepb.UnimplementedBucketServiceServer
	logger         logging.Logger
	ctrl           ctrl.Controller
	store          *dal.Store
	attrs          *attributes.Attributes
	sharedState    *state.SharedState
	receiptSigner  *receipt.Signer
	responseSigner *signing.ResponseSigner
	authCfg        internalauth.AuthConfig
}

func NewBucketServiceServer(logger logging.Logger, ctrl ctrl.Controller, s *dal.Store, attrs *attributes.Attributes, sharedState *state.SharedState, receiptSigner *receipt.Signer, responseSigner *signing.ResponseSigner, authCfg internalauth.AuthConfig) servicepb.BucketServiceServer {
	return &BucketServiceServerImpl{
		logger:         logger,
		ctrl:           ctrl,
		store:          s,
		attrs:          attrs,
		sharedState:    sharedState,
		receiptSigner:  receiptSigner,
		responseSigner: responseSigner,
		authCfg:        authCfg,
	}
}

func (impl *BucketServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeWrite); err != nil {
		return nil, err
	}

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

	// Sign response logs with server Ed25519 key (after receipt signing, since receipt is cleared before signing)
	if impl.responseSigner != nil {
		for _, log := range logs {
			log.ResponseSignature = impl.responseSigner.SignLog(log)
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
		created.PeriodId,
	)
	if err != nil {
		impl.logger.Errorf("Failed to sign receipt for tx %d: %v", tx.Id, err)
		return
	}
	log.Receipt = receiptToken
}

func (impl *BucketServiceServerImpl) ListPeriods(req *servicepb.ListPeriodsRequest, stream servicepb.BucketService_ListPeriodsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListPeriods(stream.Context())
	if err != nil {
		return fmt.Errorf("listing periods: %w", err)
	}

	if req.PageSize > 0 {
		cursor = dal.NewLimitedCursor(cursor, req.PageSize)
	}

	return sendCursorToStream(cursor, stream, "period")
}

func (impl *BucketServiceServerImpl) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	tx, err := impl.ctrl.GetTransaction(ctx, req.Ledger, req.TransactionId)
	if err != nil {
		return nil, err
	}

	resp := &servicepb.GetTransactionResponse{Transaction: tx}
	if impl.receiptSigner != nil {
		receiptToken, err := impl.computeTransactionReceipt(req.Ledger, req.TransactionId, tx)
		if err == nil {
			resp.Receipt = receiptToken
		}
	}
	return resp, nil
}

// computeTransactionReceipt computes a JWT receipt for an existing transaction
// by looking up its creation log to extract the period ID.
func (impl *BucketServiceServerImpl) computeTransactionReceipt(ledger string, txID uint64, tx *commonpb.Transaction) (string, error) {
	log, err := query.FindTransactionCreationLog(impl.store, ledger, txID)
	if err != nil {
		return "", err
	}

	applyLog := log.Payload.GetApply()
	if applyLog == nil || applyLog.Log == nil {
		return "", fmt.Errorf("not an apply log")
	}
	created := applyLog.Log.Data.GetCreatedTransaction()
	if created == nil {
		return "", fmt.Errorf("not a created transaction log")
	}

	return impl.receiptSigner.Sign(ledger, txID, tx.Postings, tx.Timestamp, created.PeriodId)
}

func (impl *BucketServiceServerImpl) ListTransactions(req *servicepb.ListTransactionsRequest, stream servicepb.BucketService_ListTransactionsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	if req.Ledger == "" {
		return fmt.Errorf("ledger name is required")
	}

	impl.logger.Debugf("ListTransactions request received for ledger %s (pageSize=%d, afterTxID=%d)",
		req.Ledger, req.PageSize, req.AfterTxId)

	cursor, err := impl.ctrl.ListTransactions(stream.Context(), req.Ledger, req.PageSize, req.AfterTxId)
	if err != nil {
		return fmt.Errorf("listing transactions: %w", err)
	}

	return sendCursorToStream(cursor, stream, "transaction")
}

func (impl *BucketServiceServerImpl) ListLedgers(req *servicepb.ListLedgersRequest, stream servicepb.BucketService_ListLedgersServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	impl.logger.Debugf("ListLedgers request received")

	cursor, err := impl.ctrl.ListLedgers(stream.Context())
	if err != nil {
		return fmt.Errorf("listing ledgers: %w", err)
	}

	if req.PageSize > 0 {
		cursor = dal.NewLimitedCursor(cursor, req.PageSize)
	}

	return sendCursorToStream(cursor, stream, "ledger")
}

func (impl *BucketServiceServerImpl) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}
	return impl.ctrl.GetLedgerByName(ctx, req.Ledger)
}

func (impl *BucketServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.ctrl.GetAccount(ctx, req.Ledger, req.Address)
}

func (impl *BucketServiceServerImpl) ListAccounts(req *servicepb.ListAccountsRequest, stream servicepb.BucketService_ListAccountsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	if req.Ledger == "" {
		return fmt.Errorf("ledger name is required")
	}

	impl.logger.Debugf("ListAccounts request received for ledger %s (pageSize=%d, afterAddress=%q, prefix=%q)",
		req.Ledger, req.PageSize, req.AfterAddress, req.Prefix)

	cursor, err := impl.ctrl.ListAccounts(stream.Context(), req.Ledger, req.PageSize, req.AfterAddress, req.Prefix)
	if err != nil {
		return fmt.Errorf("listing accounts: %w", err)
	}

	return sendCursorToStream(cursor, stream, "account")
}

func (impl *BucketServiceServerImpl) GetStoreMetrics(ctx context.Context, _ *servicepb.GetStoreMetricsRequest) (*servicepb.GetStoreMetricsResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

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
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	checker := check.NewChecker(impl.store, impl.attrs)
	return checker.Check(stream.Context(), func(event *servicepb.CheckStoreEvent) {
		_ = stream.Send(event)
	})
}

func (impl *BucketServiceServerImpl) GetAuditEntry(ctx context.Context, req *servicepb.GetAuditEntryRequest) (*auditpb.AuditEntry, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetAuditEntry(ctx, req.Sequence)
}

func (impl *BucketServiceServerImpl) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream servicepb.BucketService_ListAuditEntriesServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListAuditEntries(stream.Context(), req.AfterSequence, req.FailuresOnly, req.PageSize) //nolint:protogetter
	if err != nil {
		return fmt.Errorf("listing audit entries: %w", err)
	}

	return sendCursorToStream(cursor, stream, "audit entry")
}

func (impl *BucketServiceServerImpl) GetLog(ctx context.Context, req *servicepb.GetLogRequest) (*commonpb.Log, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetLog(ctx, req.Sequence)
}

func (impl *BucketServiceServerImpl) ListLogs(req *servicepb.ListLogsRequest, stream servicepb.BucketService_ListLogsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	var afterSequence uint64
	if req.AfterSequence != nil {
		afterSequence = *req.AfterSequence
	}

	cursor, err := impl.ctrl.ListLogs(stream.Context(), afterSequence, req.PageSize)
	if err != nil {
		return fmt.Errorf("listing logs: %w", err)
	}

	return sendCursorToStream(cursor, stream, "log")
}

func (impl *BucketServiceServerImpl) GetEventsSinks(ctx context.Context, _ *servicepb.GetEventsSinksRequest) (*servicepb.GetEventsSinksResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	sinks, err := events.ReadAllSinkConfigs(impl.store)
	if err != nil {
		return nil, fmt.Errorf("loading sink configs: %w", err)
	}

	statuses, err := events.ReadAllSinkStatuses(impl.store)
	if err != nil {
		return nil, fmt.Errorf("loading sink statuses: %w", err)
	}

	return &servicepb.GetEventsSinksResponse{
		Sinks:        sinks,
		SinkStatuses: statuses,
	}, nil
}

func (impl *BucketServiceServerImpl) GetPeriodSchedule(ctx context.Context, _ *servicepb.GetPeriodScheduleRequest) (*servicepb.GetPeriodScheduleResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	cronExpr, err := query.ReadPeriodSchedule(impl.store)
	if err != nil {
		return nil, fmt.Errorf("loading period schedule: %w", err)
	}
	return &servicepb.GetPeriodScheduleResponse{Cron: cronExpr}, nil
}

func (impl *BucketServiceServerImpl) ListSigningKeys(_ *servicepb.ListSigningKeysRequest, stream servicepb.BucketService_ListSigningKeysServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeRead); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListSigningKeys(stream.Context())
	if err != nil {
		return fmt.Errorf("listing signing keys: %w", err)
	}

	return sendCursorToStream(cursor, stream, "signing key")
}

func (impl *BucketServiceServerImpl) GetMetadataSchemaStatus(ctx context.Context, req *servicepb.GetMetadataSchemaStatusRequest) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetMetadataSchemaStatus(ctx, req.Ledger)
}

func (impl *BucketServiceServerImpl) Discovery(_ context.Context, _ *servicepb.DiscoveryRequest) (*servicepb.DiscoveryResponse, error) {
	resp := &servicepb.DiscoveryResponse{}
	if impl.responseSigner != nil {
		resp.ResponseSigning = &servicepb.ResponseSigningInfo{
			PublicKey: impl.responseSigner.PublicKey(),
			KeyId:    impl.responseSigner.KeyID(),
		}
	}
	return resp, nil
}

func RegisterBucketService(server *ggrpc.Server, ledgerServiceServer servicepb.BucketServiceServer) {
	servicepb.RegisterBucketServiceServer(server, ledgerServiceServer)
}
