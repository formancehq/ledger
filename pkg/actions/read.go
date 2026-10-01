package actions

import (
	"context"
	"errors"
	"io"
	"strconv"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	auditpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// ListLedgers collects every ledger across the cluster, following the
// x-next-cursor trailer chain so installations with more ledgers than the
// server's default page still surface them all.
func ListLedgers(ctx context.Context, client auditpb.BucketServiceClient) (map[string]*auditpb.LedgerInfo, error) {
	ledgers := make(map[string]*auditpb.LedgerInfo)

	var nextCur string
	for {
		stream, err := client.ListLedgers(ctx, &auditpb.ListLedgersRequest{
			Options: &auditpb.ListOptions{PageSize: listAllPageSize, Cursor: nextCur},
		})
		if err != nil {
			return nil, err
		}

		for {
			ledger, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				return nil, recvErr
			}
			ledgers[ledger.GetName()] = ledger
		}

		next := nextCursorFromTrailer(stream.Trailer())
		if next == "" {
			return ledgers, nil
		}
		nextCur = next
	}
}

// ListNumscripts collects every numscript from the streaming RPC, following
// the x-next-cursor trailer chain so ledgers with more numscripts than the
// server's default page still surface them all.
func ListNumscripts(ctx context.Context, client auditpb.BucketServiceClient, ledger string) ([]*auditpb.NumscriptInfo, error) {
	var (
		scripts []*auditpb.NumscriptInfo
		cursor  string
	)

	for {
		stream, err := client.ListNumscripts(ctx, &auditpb.ListNumscriptsRequest{
			Ledger:  ledger,
			Options: &auditpb.ListOptions{PageSize: listAllPageSize, Cursor: cursor},
		})
		if err != nil {
			return nil, err
		}

		for {
			info, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				return nil, recvErr
			}
			scripts = append(scripts, info)
		}

		next := nextCursorFromTrailer(stream.Trailer())
		if next == "" {
			return scripts, nil
		}

		cursor = next
	}
}

// ListNumscriptVersions returns the numscript's current latest (greatest stored
// semver) and every stored version.
func ListNumscriptVersions(ctx context.Context, client auditpb.BucketServiceClient, ledger, name string) (string, []*auditpb.NumscriptVersionEntry, error) {
	resp, err := client.ListNumscriptVersions(ctx, &auditpb.ListNumscriptVersionsRequest{
		Ledger: ledger,
		Name:   name,
	})
	if err != nil {
		return "", nil, err
	}

	return resp.GetLatestVersion(), resp.GetVersions(), nil
}

// nextCursorFromTrailer returns the opaque cursor for the following page, or
// "" when the server signaled end-of-stream (no trailer). Mirrors the
// cmdutil.NextCursorFromTrailer helper without creating a CLI-package
// dependency from pkg/actions.
func nextCursorFromTrailer(trailer metadata.MD) string {
	if vals := trailer.Get("x-next-cursor"); len(vals) > 0 {
		return vals[0]
	}

	return ""
}

// ListAllAccounts collects every account for a ledger by paginating through
// the streaming RPC. The next-page cursor is read from the server's
// x-next-cursor trailer (opaque) — the helper never depends on the cursor's
// internal encoding.
func ListAllAccounts(ctx context.Context, client auditpb.BucketServiceClient, ledgerName string) ([]*auditpb.Account, error) {
	var (
		accounts []*auditpb.Account
		cursor   string
	)

	for {
		stream, err := client.ListAccounts(ctx, &auditpb.ListAccountsRequest{
			Ledger: ledgerName,
			Options: &auditpb.ListOptions{
				PageSize: listAllPageSize,
				Cursor:   cursor,
			},
		})
		if err != nil {
			return nil, err
		}

		for {
			account, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, recvErr
			}

			accounts = append(accounts, account)
		}

		next := nextCursorFromTrailer(stream.Trailer())
		if next == "" {
			return accounts, nil
		}

		cursor = next
	}
}

// ListAllTransactions collects every transaction for a ledger by paginating
// through the streaming RPC. See ListAllAccounts for the pagination shape.
func ListAllTransactions(ctx context.Context, client auditpb.BucketServiceClient, ledgerName string) ([]*auditpb.Transaction, error) {
	var (
		transactions []*auditpb.Transaction
		cursor       string
	)

	for {
		stream, err := client.ListTransactions(ctx, &auditpb.ListTransactionsRequest{
			Ledger: ledgerName,
			Options: &auditpb.ListOptions{
				PageSize: listAllPageSize,
				Cursor:   cursor,
			},
		})
		if err != nil {
			return nil, err
		}

		for {
			tx, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, recvErr
			}

			transactions = append(transactions, tx)
		}

		next := nextCursorFromTrailer(stream.Trailer())
		if next == "" {
			return transactions, nil
		}

		cursor = next
	}
}

// ListAllLogs collects every system log for a ledger by paginating through
// the streaming RPC. Resumes from the server's x-next-cursor trailer.
func ListAllLogs(ctx context.Context, client auditpb.BucketServiceClient, ledger string) ([]*auditpb.Log, error) {
	var (
		logs   []*auditpb.Log
		cursor string
	)

	for {
		req := &auditpb.ListLogsRequest{
			Ledger: ledger,
			Options: &auditpb.ListOptions{
				PageSize: listAllPageSize,
				Cursor:   cursor,
			},
		}

		page, trailer, err := listLogsPageWithTrailer(ctx, client, req)
		if err != nil {
			return nil, err
		}

		logs = append(logs, page...)

		next := nextCursorFromTrailer(trailer)
		if next == "" {
			return logs, nil
		}

		cursor = next
	}
}

// listLogsPageWithTrailer is the trailer-aware variant of ListLogsFiltered
// used by ListAllLogs; ListLogsFiltered itself stays a single-page helper
// that drops the trailer (callers that need to follow the chain build it
// themselves via ListAllLogs or directly off the stream).
func listLogsPageWithTrailer(ctx context.Context, client auditpb.BucketServiceClient, req *auditpb.ListLogsRequest) ([]*auditpb.Log, metadata.MD, error) {
	stream, err := client.ListLogs(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	var logs []*auditpb.Log
	for {
		log, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, nil, recvErr
		}
		logs = append(logs, log)
	}

	return logs, stream.Trailer(), nil
}

// listAllPageSize is the per-call page size used by every ListAll* helper.
// It must match the server's MaxPageSize so each iteration of the loop
// drains a full page from the server. A short page is the loop's
// termination signal.
const listAllPageSize uint32 = 1000

// GetAccount retrieves a single account by address.
func GetAccount(ctx context.Context, client auditpb.BucketServiceClient, ledgerName, address string) (*auditpb.Account, error) {
	return client.GetAccount(ctx, &auditpb.GetAccountRequest{
		Ledger:  ledgerName,
		Address: address,
	})
}

// GetTransaction retrieves a single transaction by ID.
func GetTransaction(ctx context.Context, client auditpb.BucketServiceClient, ledgerName string, txID uint64) (*auditpb.GetTransactionResponse, error) {
	return client.GetTransaction(ctx, &auditpb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: txID,
	})
}

// GetLedger retrieves ledger info by name.
func GetLedger(ctx context.Context, client auditpb.BucketServiceClient, ledgerName string) (*auditpb.LedgerInfo, error) {
	return client.GetLedger(ctx, &auditpb.GetLedgerRequest{
		Ledger: ledgerName,
	})
}

// GetLedgerStats retrieves transaction and account counts for a ledger.
func GetLedgerStats(ctx context.Context, client auditpb.BucketServiceClient, ledgerName string) (*auditpb.LedgerStats, error) {
	return client.GetLedgerStats(ctx, &auditpb.GetLedgerStatsRequest{
		Ledger: ledgerName,
	})
}

// GetTemplateUsage retrieves the invocation counter and last-used timestamp
// for a Numscript template. The usagebuilder folds the counter asynchronously,
// so callers asserting on a freshly-invoked template must poll (Gomega
// Eventually / require.Eventually) rather than assume immediate visibility.
func GetTemplateUsage(ctx context.Context, client auditpb.BucketServiceClient, ledger, name string) (*auditpb.TemplateUsage, error) {
	return client.GetTemplateUsage(ctx, &auditpb.GetTemplateUsageRequest{
		Ledger: ledger,
		Name:   name,
	})
}

// GetNumscript retrieves a numscript by name and optional version ("" = latest).
func GetNumscript(ctx context.Context, client auditpb.BucketServiceClient, ledger, name, version string) (*auditpb.NumscriptInfo, error) {
	return client.GetNumscript(ctx, &auditpb.GetNumscriptRequest{
		Ledger:  ledger,
		Name:    name,
		Version: version,
	})
}

// AggregateVolumes returns aggregated volumes for a ledger.
func AggregateVolumes(ctx context.Context, client auditpb.BucketServiceClient, ledgerName string) (*auditpb.AggregateResult, error) {
	return client.AggregateVolumes(ctx, &auditpb.AggregateVolumesRequest{
		Ledger: ledgerName,
	})
}

// ListAuditEntries collects all audit entries from the streaming RPC. When
// failuresOnly is true it applies the shared filter `outcome == failure`
// (failures_only is no longer a top-level request field — EN-1241).
//
// Consistency: with failuresOnly=false the read streams the audit zone directly
// and reflects every applied entry. With failuresOnly=true the outcome filter
// is served by the asynchronous audit secondary index (EN-1339), so it is
// eventually consistent — a just-applied failure may take a short moment to
// appear. Callers that assert on a freshly-applied entry must poll (e.g.
// require.Eventually / Gomega Eventually) rather than assume immediate
// visibility; do not add time.Sleep.
func ListAuditEntries(ctx context.Context, client auditpb.BucketServiceClient, failuresOnly bool) ([]*auditpb.AuditEntry, error) {
	req := &auditpb.ListAuditEntriesRequest{}
	if failuresOnly {
		req.Options = &auditpb.ListOptions{Filter: AuditOutcomeFilter(false)}
	}

	return ListAuditEntriesWithRequest(ctx, client, req)
}

// AuditOutcomeFilter builds the QueryFilter matching audit entries by outcome:
// success=true -> `outcome == success`, success=false -> failure.
func AuditOutcomeFilter(success bool) *auditpb.QueryFilter {
	val := "failure"
	if success {
		val = "success"
	}

	return &auditpb.QueryFilter{
		Filter: &auditpb.QueryFilter_Audit{
			Audit: &auditpb.AuditCondition{
				Field: auditpb.AuditField_AUDIT_FIELD_OUTCOME,
				Condition: &auditpb.AuditCondition_StringCond{
					StringCond: &auditpb.StringCondition{
						Value: &auditpb.StringCondition_Hardcoded{Hardcoded: val},
					},
				},
			},
		},
	}
}

// ListAuditEntriesWithRequest collects all audit entries matching the given
// request by paginating through the streaming RPC. The server caps each call
// at MaxPageSize; this helper loops with the last-seen sequence as a cursor
// until the server returns a short page. The caller-supplied
// Options.PageSize / Options.Cursor on req are used to seed the first call
// and are then overwritten on each subsequent iteration.
func ListAuditEntriesWithRequest(ctx context.Context, client auditpb.BucketServiceClient, req *auditpb.ListAuditEntriesRequest) ([]*auditpb.AuditEntry, error) {
	// Field-by-field copy rather than `page := *req` — protobuf-generated
	// messages embed a sync.Mutex (in MessageState) so value copy trips
	// govet (copylocks). We only need the request fields used by the
	// underlying RPC; the per-page cursor is updated below.
	// Preserve the caller's checkpoint selection on every page request —
	// dropping it would silently turn a historical scan into a live read.
	page := &auditpb.ListAuditEntriesRequest{
		Options: &auditpb.ListOptions{
			Read:     req.GetOptions().GetRead(),
			PageSize: listAllPageSize,
			Cursor:   req.GetOptions().GetCursor(),
			Reverse:  req.GetOptions().GetReverse(),
			Filter:   req.GetOptions().GetFilter(),
		},
	}

	var entries []*auditpb.AuditEntry

	for {
		stream, err := client.ListAuditEntries(ctx, page)
		if err != nil {
			return nil, err
		}

		for {
			entry, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}

			if recvErr != nil {
				return nil, recvErr
			}

			entries = append(entries, entry)
		}

		next := nextCursorFromTrailer(stream.Trailer())
		if next == "" {
			return entries, nil
		}

		page.Options.Cursor = next
	}
}

// ListLogsFiltered collects logs matching the given request parameters.
func ListLogsFiltered(ctx context.Context, client auditpb.BucketServiceClient, req *auditpb.ListLogsRequest) ([]*auditpb.Log, error) {
	stream, err := client.ListLogs(ctx, req)
	if err != nil {
		return nil, err
	}

	var logs []*auditpb.Log
	for {
		log, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}

	return logs, nil
}

// GetMetadataSchemaStatus retrieves the declared metadata field types for a ledger.
func GetMetadataSchemaStatus(ctx context.Context, client auditpb.BucketServiceClient, ledgerName string) (*auditpb.GetMetadataSchemaStatusResponse, error) {
	return client.GetMetadataSchemaStatus(ctx, &auditpb.GetMetadataSchemaStatusRequest{
		Ledger: ledgerName,
	})
}

// AnalyzeAccounts runs the AnalyzeAccounts streaming RPC and returns the final result.
func AnalyzeAccounts(ctx context.Context, client auditpb.BucketServiceClient, ledger string, variableThreshold uint32) (*auditpb.AnalyzeAccountsResponse, error) {
	stream, err := client.AnalyzeAccounts(ctx, &auditpb.AnalyzeAccountsRequest{
		Ledger:            ledger,
		VariableThreshold: variableThreshold,
	})
	if err != nil {
		return nil, err
	}

	var result *auditpb.AnalyzeAccountsResponse
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if r := event.GetResult(); r != nil {
			result = r
		}
	}

	return result, nil
}

// AnalyzeTransactions runs the AnalyzeTransactions streaming RPC and returns the final result.
func AnalyzeTransactions(ctx context.Context, client auditpb.BucketServiceClient, ledger string) (*auditpb.AnalyzeTransactionsResponse, error) {
	stream, err := client.AnalyzeTransactions(ctx, &auditpb.AnalyzeTransactionsRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	var result *auditpb.AnalyzeTransactionsResponse
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if r := event.GetResult(); r != nil {
			result = r
		}
	}

	return result, nil
}

// GetLog retrieves a single log entry by sequence number.
func GetLog(ctx context.Context, client auditpb.BucketServiceClient, sequence uint64) (*auditpb.Log, error) {
	return client.GetLog(ctx, &auditpb.GetLogRequest{
		Sequence: sequence,
	})
}

// GetAuditEntry retrieves a single audit entry by sequence number.
func GetAuditEntry(ctx context.Context, client auditpb.BucketServiceClient, sequence uint64) (*auditpb.AuditEntry, error) {
	return client.GetAuditEntry(ctx, &auditpb.GetAuditEntryRequest{
		Sequence: sequence,
	})
}

// Discovery calls the Discovery RPC.
func Discovery(ctx context.Context, client auditpb.BucketServiceClient) (*auditpb.DiscoveryResponse, error) {
	return client.Discovery(ctx, &auditpb.DiscoveryRequest{})
}

// GetPrimaryMetrics calls the GetPrimaryMetrics RPC.
func GetPrimaryMetrics(ctx context.Context, client auditpb.BucketServiceClient) (*auditpb.GetPrimaryMetricsResponse, error) {
	return client.GetPrimaryMetrics(ctx, &auditpb.GetPrimaryMetricsRequest{})
}

// GetSecondaryMetrics calls the GetSecondaryMetrics RPC.
func GetSecondaryMetrics(ctx context.Context, client auditpb.BucketServiceClient) (*auditpb.GetSecondaryMetricsResponse, error) {
	return client.GetSecondaryMetrics(ctx, &auditpb.GetSecondaryMetricsRequest{})
}

// GetIndexStatus calls the GetIndexStatus RPC.
func GetIndexStatus(ctx context.Context, client auditpb.BucketServiceClient) (*auditpb.GetIndexStatusResponse, error) {
	return client.GetIndexStatus(ctx, &auditpb.GetIndexStatusRequest{})
}

// ListAccountsFiltered collects accounts with pagination and filter params.
func ListAccountsFiltered(ctx context.Context, client auditpb.BucketServiceClient, ledger string, pageSize uint32, afterAddress string, filter *auditpb.QueryFilter) ([]*auditpb.Account, error) {
	stream, err := client.ListAccounts(ctx, &auditpb.ListAccountsRequest{
		Ledger: ledger,
		Options: &auditpb.ListOptions{
			PageSize: pageSize,
			Cursor:   afterAddress,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	var accounts []*auditpb.Account
	for {
		account, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}

	return accounts, nil
}

// ListTransactionsFiltered collects transactions with pagination and filter params.
func ListTransactionsFiltered(ctx context.Context, client auditpb.BucketServiceClient, ledger string, pageSize uint32, afterTxID uint64, filter *auditpb.QueryFilter) ([]*auditpb.Transaction, error) {
	var cursor string
	if afterTxID > 0 {
		cursor = strconv.FormatUint(afterTxID, 10)
	}

	stream, err := client.ListTransactions(ctx, &auditpb.ListTransactionsRequest{
		Ledger: ledger,
		Options: &auditpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursor,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	var transactions []*auditpb.Transaction
	for {
		tx, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// CreatePreparedQuery creates a prepared query through BucketService.Apply,
// the single audited write entry point.
func CreatePreparedQuery(ctx context.Context, client auditpb.BucketServiceClient, name, ledger string, target auditpb.QueryTarget, filter *auditpb.QueryFilter) error {
	_, err := client.Apply(ctx, auditpb.UnsignedApplyRequest("",
		CreatePreparedQueryAction(name, ledger, target, filter)))

	return err
}

// UpdatePreparedQuery updates the filter of an existing prepared query.
func UpdatePreparedQuery(ctx context.Context, client auditpb.BucketServiceClient, ledger, name string, filter *auditpb.QueryFilter) error {
	_, err := client.Apply(ctx, auditpb.UnsignedApplyRequest("",
		UpdatePreparedQueryAction(ledger, name, filter)))

	return err
}

// DeletePreparedQuery deletes a prepared query.
func DeletePreparedQuery(ctx context.Context, client auditpb.BucketServiceClient, ledger, name string) error {
	_, err := client.Apply(ctx, auditpb.UnsignedApplyRequest("",
		DeletePreparedQueryAction(ledger, name)))

	return err
}

// CreateQueryCheckpoint takes a query checkpoint through BucketService.Apply,
// the single audited write entry point. Each invocation uses a fresh idempotency
// key, preserving the original outcome across the client's transport retries
// within the server's key retention window. A replay is historical success: it
// does not promise the checkpoint is still live or ready on the serving node.
// To retry across helper invocations, retain a caller-owned key and submit
// CreateQueryCheckpointAction with WithIdempotencyKey instead.
func CreateQueryCheckpoint(ctx context.Context, client auditpb.BucketServiceClient) (checkpointID, maxSequence uint64, err error) {
	resp, err := client.Apply(ctx, auditpb.UnsignedApplyRequest(uuid.NewString(), CreateQueryCheckpointAction()))
	if err != nil {
		return 0, 0, err
	}

	checkpointID, maxSequence, ok := GetCreatedQueryCheckpoint(resp)
	if !ok {
		return 0, 0, errors.New("checkpoint creation log not found in response")
	}

	return checkpointID, maxSequence, nil
}

// DeleteQueryCheckpoint removes a query checkpoint using a fresh idempotency key
// per invocation, so transport retries replay a committed deletion within the
// server's key retention window. A separate invocation is a new operation and
// still reports a missing checkpoint. For retries across helper invocations,
// retain a caller-owned key and use DeleteQueryCheckpointAction with
// WithIdempotencyKey instead.
func DeleteQueryCheckpoint(ctx context.Context, client auditpb.BucketServiceClient, checkpointID uint64) error {
	_, err := client.Apply(ctx, auditpb.UnsignedApplyRequest(uuid.NewString(),
		DeleteQueryCheckpointAction(checkpointID)))

	return err
}

// ListPreparedQueries lists all prepared queries for a ledger.
func ListPreparedQueries(ctx context.Context, client auditpb.BucketServiceClient, ledger string) ([]*auditpb.PreparedQuery, error) {
	resp, err := client.ListPreparedQueries(ctx, &auditpb.ListPreparedQueriesRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetQueries(), nil
}

// ExecutePreparedQuery executes a prepared query and returns the response.
func ExecutePreparedQuery(ctx context.Context, client auditpb.BucketServiceClient, ledger, queryName string, mode auditpb.QueryMode, pageSize uint32) (*auditpb.ExecutePreparedQueryResponse, error) {
	return client.ExecutePreparedQuery(ctx, &auditpb.ExecutePreparedQueryRequest{
		Ledger:    ledger,
		QueryName: queryName,
		Mode:      mode,
		PageSize:  pageSize,
	})
}

// ExecutePreparedQueryWithParams executes a prepared query with runtime parameters.
func ExecutePreparedQueryWithParams(ctx context.Context, client auditpb.BucketServiceClient, ledger, queryName string, mode auditpb.QueryMode, pageSize uint32, params map[string]*auditpb.ParameterValue) (*auditpb.ExecutePreparedQueryResponse, error) {
	return client.ExecutePreparedQuery(ctx, &auditpb.ExecutePreparedQueryRequest{
		Ledger:     ledger,
		QueryName:  queryName,
		Mode:       mode,
		PageSize:   pageSize,
		Parameters: params,
	})
}
