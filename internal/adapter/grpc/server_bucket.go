package grpc

import (
	"context"
	"fmt"
	"os"

	"github.com/formancehq/go-libs/v3/logging"
	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/application/check"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/infra/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	ggrpc "google.golang.org/grpc"
)

type BucketServiceServerImpl struct {
	servicepb.UnimplementedBucketServiceServer
	logger         logging.Logger
	ctrl           ctrl.Controller
	store          *dal.Store
	readStore      *readstore.Store
	attrs          *attributes.Attributes
	sharedState    *state.SharedState
	receiptSigner  *receipt.Signer
	responseSigner *signing.ResponseSigner
	authCfg        internalauth.AuthConfig
}

func NewBucketServiceServer(logger logging.Logger, ctrl ctrl.Controller, s *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, sharedState *state.SharedState, receiptSigner *receipt.Signer, responseSigner *signing.ResponseSigner, authCfg internalauth.AuthConfig) servicepb.BucketServiceServer {
	return &BucketServiceServerImpl{
		logger:         logger,
		ctrl:           ctrl,
		store:          s,
		readStore:      rs,
		attrs:          attrs,
		sharedState:    sharedState,
		receiptSigner:  receiptSigner,
		responseSigner: responseSigner,
		authCfg:        authCfg,
	}
}

func (impl *BucketServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	// Authenticate the token and expand scopes, but don't check a specific scope yet.
	ctx, err := internalauth.Authenticate(ctx, impl.authCfg)
	if err != nil {
		return nil, err
	}

	if len(req.Requests) == 0 {
		return nil, fmt.Errorf("at least one request is required")
	}

	// Per-request scope check: each request in the batch may require a different granular scope.
	if impl.authCfg.Enabled && impl.authCfg.CheckScopes {
		effective := internalauth.ExpandedScopesFromContext(ctx)
		for i, r := range req.Requests {
			required := internalauth.RequiredScopeForRequest(r)
			if !internalauth.HasScope(effective, required) {
				return nil, status.Errorf(codes.PermissionDenied,
					"request %d requires scope %s", i, required)
			}
		}
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
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeOpsRead); err != nil {
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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
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

// waitMinLogSequence blocks until the bbolt read index has processed at
// least the requested minimum log sequence, or the context is cancelled.
func (impl *BucketServiceServerImpl) waitMinLogSequence(ctx context.Context, minLogSequence uint64) error {
	if minLogSequence == 0 {
		return nil
	}
	return impl.readStore.WaitForSequence(ctx, minLogSequence)
}

func (impl *BucketServiceServerImpl) ListTransactions(req *servicepb.ListTransactionsRequest, stream servicepb.BucketService_ListTransactionsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
		return err
	}

	if req.Ledger == "" {
		return fmt.Errorf("ledger name is required")
	}

	if err := impl.waitMinLogSequence(stream.Context(), req.MinLogSequence); err != nil {
		return err
	}

	impl.logger.Debugf("ListTransactions request received for ledger %s (pageSize=%d, afterTxID=%d, hasFilter=%v, reverse=%v)",
		req.Ledger, req.PageSize, req.AfterTxId, req.Filter != nil, req.Reverse)

	cursor, err := impl.ctrl.ListTransactions(stream.Context(), req.Ledger, req.PageSize, req.AfterTxId, req.Filter, req.Reverse)
	if err != nil {
		return fmt.Errorf("listing transactions: %w", err)
	}

	return sendCursorToStream(cursor, stream, "transaction")
}

func (impl *BucketServiceServerImpl) ListLedgers(req *servicepb.ListLedgersRequest, stream servicepb.BucketService_ListLedgersServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return nil, err
	}

	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}
	return impl.ctrl.GetLedgerByName(ctx, req.Ledger)
}

func (impl *BucketServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.ctrl.GetAccount(ctx, req.Ledger, req.Address)
}

func (impl *BucketServiceServerImpl) ListAccounts(req *servicepb.ListAccountsRequest, stream servicepb.BucketService_ListAccountsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return err
	}

	if req.Ledger == "" {
		return fmt.Errorf("ledger name is required")
	}

	if err := impl.waitMinLogSequence(stream.Context(), req.MinLogSequence); err != nil {
		return err
	}

	impl.logger.Debugf("ListAccounts request received for ledger %s (pageSize=%d, afterAddress=%q, hasFilter=%v, reverse=%v)",
		req.Ledger, req.PageSize, req.AfterAddress, req.Filter != nil, req.Reverse)

	cursor, err := impl.ctrl.ListAccounts(stream.Context(), req.Ledger, req.PageSize, req.AfterAddress, req.Filter, req.Reverse)
	if err != nil {
		return fmt.Errorf("listing accounts: %w", err)
	}

	return sendCursorToStream(cursor, stream, "account")
}

func (impl *BucketServiceServerImpl) GetStoreMetrics(ctx context.Context, _ *servicepb.GetStoreMetricsRequest) (*servicepb.GetStoreMetricsResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
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

func (impl *BucketServiceServerImpl) GetIndexStatus(ctx context.Context, _ *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	lastIndexed, err := impl.readStore.LastIndexedSequence()
	if err != nil {
		return nil, fmt.Errorf("reading last indexed sequence: %w", err)
	}

	lastLog, err := query.ReadLastSequence(impl.store)
	if err != nil {
		return nil, fmt.Errorf("reading last log sequence: %w", err)
	}

	var lag uint64
	if lastLog > lastIndexed {
		lag = lastLog - lastIndexed
	}

	var fileSize uint64
	if info, err := os.Stat(impl.readStore.Path()); err == nil {
		fileSize = uint64(info.Size())
	}

	return &servicepb.GetIndexStatusResponse{
		LastIndexedSequence: lastIndexed,
		LastLogSequence:     lastLog,
		Lag:                 lag,
		IndexFileSize:       fileSize,
	}, nil
}

func (impl *BucketServiceServerImpl) CheckStore(_ *servicepb.CheckStoreRequest, stream servicepb.BucketService_CheckStoreServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	checker := check.NewChecker(impl.store, impl.attrs)
	return checker.Check(stream.Context(), func(event *servicepb.CheckStoreEvent) {
		_ = stream.Send(event)
	})
}

func (impl *BucketServiceServerImpl) GetAuditEntry(ctx context.Context, req *servicepb.GetAuditEntryRequest) (*auditpb.AuditEntry, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAuditRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetAuditEntry(ctx, req.Sequence)
}

func (impl *BucketServiceServerImpl) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream servicepb.BucketService_ListAuditEntriesServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeAuditRead); err != nil {
		return err
	}

	if err := impl.waitMinLogSequence(stream.Context(), req.MinLogSequence); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListAuditEntries(stream.Context(), req.AfterSequence, req.FailuresOnly, req.PageSize) //nolint:protogetter
	if err != nil {
		return fmt.Errorf("listing audit entries: %w", err)
	}

	return sendCursorToStream(cursor, stream, "audit entry")
}

func (impl *BucketServiceServerImpl) GetLog(ctx context.Context, req *servicepb.GetLogRequest) (*commonpb.Log, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetLog(ctx, req.Sequence)
}

func (impl *BucketServiceServerImpl) ListLogs(req *servicepb.ListLogsRequest, stream servicepb.BucketService_ListLogsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	if err := impl.waitMinLogSequence(stream.Context(), req.MinLogSequence); err != nil {
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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	sinks, err := query.ReadAllSinkConfigs(impl.store)
	if err != nil {
		return nil, fmt.Errorf("loading sink configs: %w", err)
	}

	statuses, err := query.ReadAllSinkStatuses(impl.store)
	if err != nil {
		return nil, fmt.Errorf("loading sink statuses: %w", err)
	}

	return &servicepb.GetEventsSinksResponse{
		Sinks:        sinks,
		SinkStatuses: statuses,
	}, nil
}

func (impl *BucketServiceServerImpl) GetPeriodSchedule(ctx context.Context, _ *servicepb.GetPeriodScheduleRequest) (*servicepb.GetPeriodScheduleResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	cronExpr, err := query.ReadPeriodSchedule(impl.store)
	if err != nil {
		return nil, fmt.Errorf("loading period schedule: %w", err)
	}
	return &servicepb.GetPeriodScheduleResponse{Cron: cronExpr}, nil
}

func (impl *BucketServiceServerImpl) ListSigningKeys(_ *servicepb.ListSigningKeysRequest, stream servicepb.BucketService_ListSigningKeysServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListSigningKeys(stream.Context())
	if err != nil {
		return fmt.Errorf("listing signing keys: %w", err)
	}

	return sendCursorToStream(cursor, stream, "signing key")
}

func (impl *BucketServiceServerImpl) GetMetadataSchemaStatus(ctx context.Context, req *servicepb.GetMetadataSchemaStatusRequest) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetMetadataSchemaStatus(ctx, req.Ledger)
}

func (impl *BucketServiceServerImpl) AnalyzeAccounts(ctx context.Context, req *servicepb.AnalyzeAccountsRequest) (*servicepb.AnalyzeAccountsResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.ctrl.AnalyzeAccounts(ctx, req.Ledger, req.VariableThreshold)
}

func (impl *BucketServiceServerImpl) CreatePreparedQuery(ctx context.Context, req *servicepb.CreatePreparedQueryRequest) (*servicepb.CreatePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesWrite); err != nil {
		return nil, err
	}

	_, err := impl.ctrl.Apply(ctx, &servicepb.Request{
		Type: &servicepb.Request_CreatePreparedQuery{
			CreatePreparedQuery: req,
		},
	})
	if err != nil {
		return nil, err
	}
	return &servicepb.CreatePreparedQueryResponse{}, nil
}

func (impl *BucketServiceServerImpl) UpdatePreparedQuery(ctx context.Context, req *servicepb.UpdatePreparedQueryRequest) (*servicepb.UpdatePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesWrite); err != nil {
		return nil, err
	}

	_, err := impl.ctrl.Apply(ctx, &servicepb.Request{
		Type: &servicepb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: req,
		},
	})
	if err != nil {
		return nil, err
	}
	return &servicepb.UpdatePreparedQueryResponse{}, nil
}

func (impl *BucketServiceServerImpl) DeletePreparedQuery(ctx context.Context, req *servicepb.DeletePreparedQueryRequest) (*servicepb.DeletePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesWrite); err != nil {
		return nil, err
	}

	_, err := impl.ctrl.Apply(ctx, &servicepb.Request{
		Type: &servicepb.Request_DeletePreparedQuery{
			DeletePreparedQuery: req,
		},
	})
	if err != nil {
		return nil, err
	}
	return &servicepb.DeletePreparedQueryResponse{}, nil
}

func (impl *BucketServiceServerImpl) ListPreparedQueries(ctx context.Context, req *servicepb.ListPreparedQueriesRequest) (*servicepb.ListPreparedQueriesResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesRead); err != nil {
		return nil, err
	}

	queries, err := impl.ctrl.ListPreparedQueries(ctx, req.Ledger)
	if err != nil {
		return nil, err
	}
	return &servicepb.ListPreparedQueriesResponse{Queries: queries}, nil
}

func (impl *BucketServiceServerImpl) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesRead); err != nil {
		return nil, err
	}

	return impl.ctrl.ExecutePreparedQuery(ctx, req)
}

func (impl *BucketServiceServerImpl) GetLedgerStats(ctx context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return nil, err
	}

	if req.Ledger == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	return impl.ctrl.GetLedgerStats(ctx, req.Ledger)
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
