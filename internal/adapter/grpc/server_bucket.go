package grpc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/application/check"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

var bucketTracer = otel.Tracer("grpc.bucket")

const (
	metadataKeyQueryProfile       = "x-query-profile"
	metadataKeyQueryProfileResult = "x-query-profile-result-bin"
)

type BucketServiceServerImpl struct {
	servicepb.UnimplementedBucketServiceServer

	logger                logging.Logger
	ctrl                  ctrl.Controller
	store                 *dal.Store
	readStore             *readstore.Store
	attrs                 *attributes.Attributes
	sharedState           *state.SharedState
	receiptSigner         *receipt.Signer
	responseSigner        *signing.ResponseSigner
	authCfg               internalauth.AuthConfig
	queryProfileThreshold time.Duration
}

func NewBucketServiceServer(logger logging.Logger, ctrl ctrl.Controller, s *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, sharedState *state.SharedState, receiptSigner *receipt.Signer, responseSigner *signing.ResponseSigner, authCfg internalauth.AuthConfig, queryProfileThreshold time.Duration) servicepb.BucketServiceServer {
	return &BucketServiceServerImpl{
		logger:                logger,
		ctrl:                  ctrl,
		store:                 s,
		readStore:             rs,
		attrs:                 attrs,
		sharedState:           sharedState,
		receiptSigner:         receiptSigner,
		responseSigner:        responseSigner,
		authCfg:               authCfg,
		queryProfileThreshold: queryProfileThreshold,
	}
}

func (impl *BucketServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	// Authenticate the token and expand scopes, but don't check a specific scope yet.
	ctx, err := internalauth.Authenticate(ctx, impl.authCfg)
	if err != nil {
		return nil, err
	}

	if len(req.GetRequests()) == 0 {
		return nil, errors.New("at least one request is required")
	}

	// Per-request scope check: each request in the batch may require a different granular scope.
	if impl.authCfg.Enabled {
		effective := internalauth.ExpandedScopesFromContext(ctx)

		for i, r := range req.GetRequests() {
			required := internalauth.RequiredScopeForRequest(r)
			if !internalauth.HasScope(effective, required) {
				return nil, status.Errorf(codes.PermissionDenied,
					"request %d requires scope %s", i, required)
			}
		}
	}

	impl.logger.Debugf("Apply request received with %d requests", len(req.GetRequests()))

	logs, err := impl.ctrl.Apply(ctx, req.GetRequests()...)
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
	applyLog := log.GetPayload().GetApply()
	if applyLog == nil || applyLog.GetLog() == nil {
		return
	}

	created := applyLog.GetLog().GetData().GetCreatedTransaction()
	if created == nil || created.GetTransaction() == nil {
		return
	}

	tx := created.GetTransaction()

	receiptToken, err := impl.receiptSigner.Sign(
		applyLog.GetLedgerName(),
		tx.GetId(),
		tx.GetPostings(),
		tx.GetTimestamp(),
		created.GetPeriodId(),
	)
	if err != nil {
		impl.logger.Errorf("Failed to sign receipt for tx %d: %v", tx.GetId(), err)

		return
	}

	log.Receipt = receiptToken
}

func (impl *BucketServiceServerImpl) ListPeriods(req *servicepb.ListPeriodsRequest, stream servicepb.BucketService_ListPeriodsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListPeriods")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListPeriods(ctx)
	if err != nil {
		return fmt.Errorf("listing periods: %w", err)
	}

	if req.GetPageSize() > 0 {
		cursor = dal.NewLimitedCursor(cursor, req.GetPageSize())
	}

	return sendCursorToStream(ctx, cursor, stream, "period")
}

func (impl *BucketServiceServerImpl) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, errors.New("ledger name is required")
	}

	tx, err := impl.ctrl.GetTransaction(ctx, req.GetLedger(), req.GetTransactionId())
	if err != nil {
		return nil, err
	}

	resp := &servicepb.GetTransactionResponse{Transaction: tx}
	if impl.receiptSigner != nil {
		receiptToken, err := impl.computeTransactionReceipt(ctx, req.GetLedger(), req.GetTransactionId(), tx)
		if err == nil {
			resp.Receipt = receiptToken
		}
	}

	return resp, nil
}

// computeTransactionReceipt computes a JWT receipt for an existing transaction
// by looking up its creation log to extract the period ID.
func (impl *BucketServiceServerImpl) computeTransactionReceipt(ctx context.Context, ledger string, txID uint64, tx *commonpb.Transaction) (string, error) {
	log, err := query.FindTransactionCreationLog(ctx, impl.store, impl.attrs.Transaction, ledger, txID)
	if err != nil {
		return "", err
	}

	applyLog := log.GetPayload().GetApply()
	if applyLog == nil || applyLog.GetLog() == nil {
		return "", errors.New("not an apply log")
	}

	created := applyLog.GetLog().GetData().GetCreatedTransaction()
	if created == nil {
		return "", errors.New("not a created transaction log")
	}

	return impl.receiptSigner.Sign(ledger, txID, tx.GetPostings(), tx.GetTimestamp(), created.GetPeriodId())
}

// waitMinLogSequence blocks until the Pebble read index has processed at
// least the requested minimum log sequence, or the context is cancelled.
func (impl *BucketServiceServerImpl) waitMinLogSequence(ctx context.Context, minLogSequence uint64) error {
	if minLogSequence == 0 {
		return nil
	}

	return impl.readStore.WaitForSequence(ctx, minLogSequence)
}

func (impl *BucketServiceServerImpl) ListTransactions(req *servicepb.ListTransactionsRequest, stream servicepb.BucketService_ListTransactionsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListTransactions",
		trace.WithAttributes(attribute.String("ledger", req.GetLedger())))
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
		return err
	}

	if req.GetLedger() == "" {
		return errors.New("ledger name is required")
	}

	if err := impl.waitMinLogSequence(ctx, req.GetMinLogSequence()); err != nil {
		return err
	}

	impl.logger.Debugf("ListTransactions request received for ledger %s (pageSize=%d, afterTxID=%d, hasFilter=%v, reverse=%v)",
		req.GetLedger(), req.GetPageSize(), req.GetAfterTxId(), req.GetFilter() != nil, req.GetReverse())

	profileCtx, profile := query.WithProfile(ctx)

	cursor, err := impl.ctrl.ListTransactions(profileCtx, req.GetLedger(), req.GetPageSize(), req.GetAfterTxId(), req.GetFilter(), req.GetReverse())
	if err != nil {
		return fmt.Errorf("listing transactions: %w", err)
	}

	err = sendCursorToStream(ctx, cursor, stream, "transaction")
	impl.emitProfile(ctx, profile)

	return err
}

func (impl *BucketServiceServerImpl) ListLedgers(req *servicepb.ListLedgersRequest, stream servicepb.BucketService_ListLedgersServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListLedgers")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListLedgers(ctx)
	if err != nil {
		return fmt.Errorf("listing ledgers: %w", err)
	}

	if req.GetPageSize() > 0 {
		cursor = dal.NewLimitedCursor(cursor, req.GetPageSize())
	}

	return sendCursorToStream(ctx, cursor, stream, "ledger")
}

func (impl *BucketServiceServerImpl) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	ctx, span := bucketTracer.Start(ctx, "grpc.GetLedger")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, errors.New("ledger name is required")
	}

	return impl.ctrl.GetLedgerByName(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, errors.New("ledger name is required")
	}

	return impl.ctrl.GetAccount(ctx, req.GetLedger(), req.GetAddress())
}

func (impl *BucketServiceServerImpl) ListAccounts(req *servicepb.ListAccountsRequest, stream servicepb.BucketService_ListAccountsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListAccounts",
		trace.WithAttributes(attribute.String("ledger", req.GetLedger())))
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return err
	}

	if req.GetLedger() == "" {
		return errors.New("ledger name is required")
	}

	if err := impl.waitMinLogSequence(ctx, req.GetMinLogSequence()); err != nil {
		return err
	}

	impl.logger.Debugf("ListAccounts request received for ledger %s (pageSize=%d, afterAddress=%q, hasFilter=%v, reverse=%v)",
		req.GetLedger(), req.GetPageSize(), req.GetAfterAddress(), req.GetFilter() != nil, req.GetReverse())

	profileCtx, profile := query.WithProfile(ctx)

	cursor, err := impl.ctrl.ListAccounts(profileCtx, req.GetLedger(), req.GetPageSize(), req.GetAfterAddress(), req.GetFilter(), req.GetReverse())
	if err != nil {
		return fmt.Errorf("listing accounts: %w", err)
	}

	err = sendCursorToStream(ctx, cursor, stream, "account")
	impl.emitProfile(ctx, profile)

	return err
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

func (impl *BucketServiceServerImpl) GetReadIndexMetrics(ctx context.Context, _ *servicepb.GetReadIndexMetricsRequest) (*servicepb.GetReadIndexMetricsResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	if impl.readStore == nil {
		return &servicepb.GetReadIndexMetricsResponse{
			Available: false,
		}, nil
	}

	return &servicepb.GetReadIndexMetricsResponse{
		Available: true,
		Metrics:   impl.readStore.GetMetrics(),
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

	// Read per-index backfill progress from Pebble.
	backfillEntries, err := impl.readStore.ListBackfillProgress()
	if err != nil {
		return nil, fmt.Errorf("reading backfill progress: %w", err)
	}

	progress := make([]*servicepb.IndexBackfillProgress, 0, len(backfillEntries))
	for _, e := range backfillEntries {
		entry := &servicepb.IndexBackfillProgress{
			Ledger: e.Ledger,
			Cursor: e.Cursor,
		}
		switch e.Kind {
		case readstore.BackfillKindTxBuiltin:
			if len(e.Details) >= 1 {
				entry.Index = &servicepb.IndexBackfillProgress_Transaction{
					Transaction: &commonpb.TransactionIndex{
						Kind: &commonpb.TransactionIndex_Builtin{
							Builtin: commonpb.TransactionBuiltinIndex(e.Details[0]),
						},
					},
				}
			}
		case readstore.BackfillKindTxMetadata:
			entry.Index = &servicepb.IndexBackfillProgress_Transaction{
				Transaction: &commonpb.TransactionIndex{
					Kind: &commonpb.TransactionIndex_MetadataKey{
						MetadataKey: string(e.Details),
					},
				},
			}
		case readstore.BackfillKindAcctBuiltin:
			if len(e.Details) >= 1 {
				entry.Index = &servicepb.IndexBackfillProgress_Account{
					Account: &commonpb.AccountIndex{
						Kind: &commonpb.AccountIndex_Builtin{
							Builtin: commonpb.AccountBuiltinIndex(e.Details[0]),
						},
					},
				}
			}
		case readstore.BackfillKindAcctMetadata:
			entry.Index = &servicepb.IndexBackfillProgress_Account{
				Account: &commonpb.AccountIndex{
					Kind: &commonpb.AccountIndex_MetadataKey{
						MetadataKey: string(e.Details),
					},
				},
			}
		case readstore.BackfillKindLogBuiltin:
			if len(e.Details) >= 1 {
				entry.Index = &servicepb.IndexBackfillProgress_LogBuiltin{
					LogBuiltin: commonpb.LogBuiltinIndex(e.Details[0]),
				}
			}
		}

		progress = append(progress, entry)
	}

	return &servicepb.GetIndexStatusResponse{
		LastIndexedSequence: lastIndexed,
		LastLogSequence:     lastLog,
		Lag:                 lag,
		IndexFileSize:       fileSize,
		BackfillProgress:    progress,
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

	return impl.ctrl.GetAuditEntry(ctx, req.GetSequence())
}

func (impl *BucketServiceServerImpl) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream servicepb.BucketService_ListAuditEntriesServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListAuditEntries")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAuditRead); err != nil {
		return err
	}

	if err := impl.waitMinLogSequence(ctx, req.GetMinLogSequence()); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListAuditEntries(ctx, req.AfterSequence, req.FailuresOnly, req.PageSize) //nolint:protogetter
	if err != nil {
		return fmt.Errorf("listing audit entries: %w", err)
	}

	return sendCursorToStream(ctx, cursor, stream, "audit entry")
}

func (impl *BucketServiceServerImpl) GetLog(ctx context.Context, req *servicepb.GetLogRequest) (*commonpb.Log, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetLog(ctx, req.GetSequence())
}

func (impl *BucketServiceServerImpl) ListLogs(req *servicepb.ListLogsRequest, stream servicepb.BucketService_ListLogsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListLogs")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	if err := impl.waitMinLogSequence(ctx, req.GetMinLogSequence()); err != nil {
		return err
	}

	var afterSequence uint64
	if req.AfterSequence != nil {
		afterSequence = req.GetAfterSequence()
	}

	cursor, err := impl.ctrl.ListLogs(ctx, afterSequence, req.GetPageSize(), req.GetFilter())
	if err != nil {
		return fmt.Errorf("listing logs: %w", err)
	}

	return sendCursorToStream(ctx, cursor, stream, "log")
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
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListSigningKeys")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	cursor, err := impl.ctrl.ListSigningKeys(ctx)
	if err != nil {
		return fmt.Errorf("listing signing keys: %w", err)
	}

	return sendCursorToStream(ctx, cursor, stream, "signing key")
}

func (impl *BucketServiceServerImpl) GetMetadataSchemaStatus(ctx context.Context, req *servicepb.GetMetadataSchemaStatusRequest) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetMetadataSchemaStatus(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) AnalyzeAccounts(req *servicepb.AnalyzeAccountsRequest, stream servicepb.BucketService_AnalyzeAccountsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return err
	}

	if req.GetLedger() == "" {
		return errors.New("ledger name is required")
	}

	onProgress := func(processed, total uint64) {
		_ = stream.Send(&servicepb.AnalyzeAccountsEvent{
			Type: &servicepb.AnalyzeAccountsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{
					Processed: processed,
					Total:     total,
					Phase:     "scanning",
				},
			},
		})
	}

	resp, err := impl.ctrl.AnalyzeAccounts(stream.Context(), req.GetLedger(), req.GetVariableThreshold(), onProgress)
	if err != nil {
		return err
	}

	return stream.Send(&servicepb.AnalyzeAccountsEvent{
		Type: &servicepb.AnalyzeAccountsEvent_Result{Result: resp},
	})
}

func (impl *BucketServiceServerImpl) AnalyzeTransactions(req *servicepb.AnalyzeTransactionsRequest, stream servicepb.BucketService_AnalyzeTransactionsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
		return err
	}

	if req.GetLedger() == "" {
		return errors.New("ledger name is required")
	}

	onProgress := func(processed, total uint64) {
		_ = stream.Send(&servicepb.AnalyzeTransactionsEvent{
			Type: &servicepb.AnalyzeTransactionsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{
					Processed: processed,
					Total:     total,
				},
			},
		})
	}

	resp, err := impl.ctrl.AnalyzeTransactions(stream.Context(), req.GetLedger(), req.GetVariableThreshold(), onProgress)
	if err != nil {
		return err
	}

	return stream.Send(&servicepb.AnalyzeTransactionsEvent{
		Type: &servicepb.AnalyzeTransactionsEvent_Result{Result: resp},
	})
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

	queries, err := impl.ctrl.ListPreparedQueries(ctx, req.GetLedger())
	if err != nil {
		return nil, err
	}

	return &servicepb.ListPreparedQueriesResponse{Queries: queries}, nil
}

func (impl *BucketServiceServerImpl) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesRead); err != nil {
		return nil, err
	}

	profileCtx, profile := query.WithProfile(ctx)

	resp, err := impl.ctrl.ExecutePreparedQuery(profileCtx, req)
	impl.emitProfile(ctx, profile)

	return resp, err
}

func (impl *BucketServiceServerImpl) GetLedgerStats(ctx context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, errors.New("ledger name is required")
	}

	return impl.ctrl.GetLedgerStats(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) AggregateVolumes(ctx context.Context, req *servicepb.AggregateVolumesRequest) (*commonpb.AggregateResult, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, errors.New("ledger name is required")
	}

	if err := impl.waitMinLogSequence(ctx, req.GetMinLogSequence()); err != nil {
		return nil, err
	}

	profileCtx, profile := query.WithProfile(ctx)

	result, err := impl.ctrl.AggregateVolumes(profileCtx, req.GetLedger(), req.GetFilter())
	impl.emitProfile(ctx, profile)

	return result, err
}

func (impl *BucketServiceServerImpl) GetNumscript(ctx context.Context, req *servicepb.GetNumscriptRequest) (*commonpb.NumscriptInfo, error) {
	return impl.ctrl.GetNumscript(ctx, req.GetLedger(), req.GetName(), req.GetVersion())
}

func (impl *BucketServiceServerImpl) ListNumscripts(req *servicepb.ListNumscriptsRequest, stream servicepb.BucketService_ListNumscriptsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListNumscripts")
	defer span.End()

	scripts, err := impl.ctrl.ListNumscripts(ctx, req.GetLedger())
	if err != nil {
		return fmt.Errorf("listing numscripts: %w", err)
	}

	for _, script := range scripts {
		err := stream.Send(script)
		if err != nil {
			return fmt.Errorf("sending numscript: %w", err)
		}
	}

	return nil
}

func (impl *BucketServiceServerImpl) Discovery(_ context.Context, _ *servicepb.DiscoveryRequest) (*servicepb.DiscoveryResponse, error) {
	resp := &servicepb.DiscoveryResponse{}
	if impl.responseSigner != nil {
		resp.ResponseSigning = &servicepb.ResponseSigningInfo{
			PublicKey: impl.responseSigner.PublicKey(),
			KeyId:     impl.responseSigner.KeyID(),
		}
	}

	return resp, nil
}

func (impl *BucketServiceServerImpl) emitProfile(ctx context.Context, profile *query.QueryProfile) {
	if profile == nil {
		return
	}

	if profile.TotalDuration() >= impl.queryProfileThreshold {
		profile.LogTo(impl.logger)
		profile.EmitToSpan(trace.SpanFromContext(ctx))
	}

	if wantsProfile(ctx) {
		_ = ggrpc.SetTrailer(ctx, profileToMetadata(profile))
	}
}

func wantsProfile(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)

	return ok && len(md.Get(metadataKeyQueryProfile)) > 0
}

func profileToMetadata(profile *query.QueryProfile) metadata.MD {
	pb := profile.ToProto()

	data, err := proto.Marshal(pb)
	if err != nil {
		return nil
	}

	return metadata.Pairs(metadataKeyQueryProfileResult, string(data))
}

func RegisterBucketService(server *ggrpc.Server, ledgerServiceServer servicepb.BucketServiceServer) {
	servicepb.RegisterBucketServiceServer(server, ledgerServiceServer)
}
