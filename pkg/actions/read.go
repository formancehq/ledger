package actions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// ListLedgers collects every ledger across the cluster, following the
// x-next-cursor trailer chain so installations with more ledgers than the
// server's default page still surface them all.
func ListLedgers(ctx context.Context, client servicepb.BucketServiceClient) (map[string]*commonpb.LedgerInfo, error) {
	ledgers := make(map[string]*commonpb.LedgerInfo)

	var nextCur string
	for {
		stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{
			Options: &commonpb.ListOptions{PageSize: listAllPageSize, Cursor: nextCur},
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
func ListNumscripts(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]*commonpb.NumscriptInfo, error) {
	var (
		scripts []*commonpb.NumscriptInfo
		cursor  string
	)

	for {
		stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{
			Ledger:  ledger,
			Options: &commonpb.ListOptions{PageSize: listAllPageSize, Cursor: cursor},
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
func ListAllAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) ([]*commonpb.Account, error) {
	var (
		accounts []*commonpb.Account
		cursor   string
	)

	for {
		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
			Ledger: ledgerName,
			Options: &commonpb.ListOptions{
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
func ListAllTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) ([]*commonpb.Transaction, error) {
	var (
		transactions []*commonpb.Transaction
		cursor       string
	)

	for {
		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger: ledgerName,
			Options: &commonpb.ListOptions{
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
func ListAllLogs(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]*commonpb.Log, error) {
	var (
		logs   []*commonpb.Log
		cursor string
	)

	for {
		req := &servicepb.ListLogsRequest{
			Ledger: ledger,
			Options: &commonpb.ListOptions{
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
func listLogsPageWithTrailer(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ListLogsRequest) ([]*commonpb.Log, metadata.MD, error) {
	stream, err := client.ListLogs(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	var logs []*commonpb.Log
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

// ListAllChapters collects every chapter across the cluster, following the
// x-next-cursor trailer chain so installations with more chapters than the
// server's default page still surface them all.
func ListAllChapters(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Chapter, error) {
	var (
		chapters []*commonpb.Chapter
		nextCur  string
	)

	for {
		stream, err := client.ListChapters(ctx, &servicepb.ListChaptersRequest{
			Options: &commonpb.ListOptions{PageSize: listAllPageSize, Cursor: nextCur},
		})
		if err != nil {
			return nil, err
		}

		for {
			chapter, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				return nil, recvErr
			}
			chapters = append(chapters, chapter)
		}

		next := nextCursorFromTrailer(stream.Trailer())
		if next == "" {
			return chapters, nil
		}
		nextCur = next
	}
}

// GetAccount retrieves a single account by address.
func GetAccount(ctx context.Context, client servicepb.BucketServiceClient, ledgerName, address string) (*commonpb.Account, error) {
	return client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledgerName,
		Address: address,
	})
}

// GetTransaction retrieves a single transaction by ID.
func GetTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, txID uint64) (*servicepb.GetTransactionResponse, error) {
	return client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledgerName,
		TransactionId: txID,
	})
}

// GetLedger retrieves ledger info by name.
func GetLedger(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*commonpb.LedgerInfo, error) {
	return client.GetLedger(ctx, &servicepb.GetLedgerRequest{
		Ledger: ledgerName,
	})
}

// GetLedgerStats retrieves transaction and account counts for a ledger.
func GetLedgerStats(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*commonpb.LedgerStats, error) {
	return client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{
		Ledger: ledgerName,
	})
}

// GetNumscript retrieves a numscript by name and optional version ("" = latest).
func GetNumscript(ctx context.Context, client servicepb.BucketServiceClient, ledger, name, version string) (*commonpb.NumscriptInfo, error) {
	return client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
		Ledger:  ledger,
		Name:    name,
		Version: version,
	})
}

// AggregateVolumes returns aggregated volumes for a ledger.
func AggregateVolumes(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*commonpb.AggregateResult, error) {
	return client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{
		Ledger: ledgerName,
	})
}

// ListAuditEntries collects all audit entries from the streaming RPC. When
// failuresOnly is set, it scopes the scan to failed orders via the audit
// outcome filter (the server-side FailuresOnly field was replaced by the
// generic options.filter — see EN-1305).
func ListAuditEntries(ctx context.Context, client servicepb.BucketServiceClient, failuresOnly bool) ([]*auditpb.AuditEntry, error) {
	var filter *commonpb.QueryFilter
	if failuresOnly {
		parsed, err := filterexpr.Parse("audit[outcome] == failure")
		if err != nil {
			return nil, fmt.Errorf("building audit failures filter: %w", err)
		}
		filter = parsed
	}

	return ListAuditEntriesWithRequest(ctx, client, &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			Filter: filter,
		},
	})
}

// ListAuditEntriesWithRequest collects all audit entries matching the given
// request by paginating through the streaming RPC. The server caps each call
// at MaxPageSize; this helper loops with the last-seen sequence as a cursor
// until the server returns a short page. The caller-supplied
// Options.PageSize / Options.Cursor on req are used to seed the first call
// and are then overwritten on each subsequent iteration.
func ListAuditEntriesWithRequest(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ListAuditEntriesRequest) ([]*auditpb.AuditEntry, error) {
	// Field-by-field copy rather than `page := *req` — protobuf-generated
	// messages embed a sync.Mutex (in MessageState) so value copy trips
	// govet (copylocks). We only need the request fields used by the
	// underlying RPC; the per-page cursor is updated below.
	// Preserve the caller's Read (min_log_sequence, checkpoint_id) and Filter
	// on every page request — dropping Read would silently turn a
	// freshness-gated audit scan into a stale read against a lagging follower,
	// and dropping Filter would widen the scan past the caller's predicate.
	page := &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			Read:     req.GetOptions().GetRead(),
			PageSize: listAllPageSize,
			Cursor:   req.GetOptions().GetCursor(),
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
func ListLogsFiltered(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ListLogsRequest) ([]*commonpb.Log, error) {
	stream, err := client.ListLogs(ctx, req)
	if err != nil {
		return nil, err
	}

	var logs []*commonpb.Log
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

// GetMetadataSchemaStatus retrieves the metadata schema conversion status for a ledger.
func GetMetadataSchemaStatus(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	return client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
		Ledger: ledgerName,
	})
}

// GetChapterSchedule retrieves the current chapter schedule cron expression.
func GetChapterSchedule(ctx context.Context, client servicepb.BucketServiceClient) (string, error) {
	resp, err := client.GetChapterSchedule(ctx, &servicepb.GetChapterScheduleRequest{})
	if err != nil {
		return "", err
	}

	return resp.GetCron(), nil
}

// AnalyzeAccounts runs the AnalyzeAccounts streaming RPC and returns the final result.
func AnalyzeAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledger string, variableThreshold uint32) (*servicepb.AnalyzeAccountsResponse, error) {
	stream, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
		Ledger:            ledger,
		VariableThreshold: variableThreshold,
	})
	if err != nil {
		return nil, err
	}

	var result *servicepb.AnalyzeAccountsResponse
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
func AnalyzeTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (*servicepb.AnalyzeTransactionsResponse, error) {
	stream, err := client.AnalyzeTransactions(ctx, &servicepb.AnalyzeTransactionsRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	var result *servicepb.AnalyzeTransactionsResponse
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
func GetLog(ctx context.Context, client servicepb.BucketServiceClient, sequence uint64) (*commonpb.Log, error) {
	return client.GetLog(ctx, &servicepb.GetLogRequest{
		Sequence: sequence,
	})
}

// GetAuditEntry retrieves a single audit entry by sequence number.
func GetAuditEntry(ctx context.Context, client servicepb.BucketServiceClient, sequence uint64) (*auditpb.AuditEntry, error) {
	return client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{
		Sequence: sequence,
	})
}

// Discovery calls the Discovery RPC.
func Discovery(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.DiscoveryResponse, error) {
	return client.Discovery(ctx, &servicepb.DiscoveryRequest{})
}

// GetPrimaryMetrics calls the GetPrimaryMetrics RPC.
func GetPrimaryMetrics(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.GetPrimaryMetricsResponse, error) {
	return client.GetPrimaryMetrics(ctx, &servicepb.GetPrimaryMetricsRequest{})
}

// GetSecondaryMetrics calls the GetSecondaryMetrics RPC.
func GetSecondaryMetrics(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.GetSecondaryMetricsResponse, error) {
	return client.GetSecondaryMetrics(ctx, &servicepb.GetSecondaryMetricsRequest{})
}

// GetIndexStatus calls the GetIndexStatus RPC.
func GetIndexStatus(ctx context.Context, client servicepb.BucketServiceClient) (*servicepb.GetIndexStatusResponse, error) {
	return client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{})
}

// ListAccountsFiltered collects accounts with pagination and filter params.
func ListAccountsFiltered(ctx context.Context, client servicepb.BucketServiceClient, ledger string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter) ([]*commonpb.Account, error) {
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: pageSize,
			Cursor:   afterAddress,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	var accounts []*commonpb.Account
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
func ListTransactionsFiltered(ctx context.Context, client servicepb.BucketServiceClient, ledger string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter) ([]*commonpb.Transaction, error) {
	var cursor string
	if afterTxID > 0 {
		cursor = strconv.FormatUint(afterTxID, 10)
	}

	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: pageSize,
			Cursor:   cursor,
			Filter:   filter,
		},
	})
	if err != nil {
		return nil, err
	}

	var transactions []*commonpb.Transaction
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

// CreatePreparedQuery creates a prepared query via the gRPC API.
func CreatePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, name, ledger string, target commonpb.QueryTarget, filter *commonpb.QueryFilter) error {
	_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
		Ledger: ledger,
		Query: &commonpb.PreparedQuery{
			Name:   name,
			Target: target,
			Filter: filter,
		},
	})

	return err
}

// UpdatePreparedQuery updates the filter of an existing prepared query.
func UpdatePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, ledger, name string, filter *commonpb.QueryFilter) error {
	_, err := client.UpdatePreparedQuery(ctx, &servicepb.UpdatePreparedQueryRequest{
		Ledger: ledger,
		Name:   name,
		Filter: filter,
	})

	return err
}

// DeletePreparedQuery deletes a prepared query.
func DeletePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, ledger, name string) error {
	_, err := client.DeletePreparedQuery(ctx, &servicepb.DeletePreparedQueryRequest{
		Ledger: ledger,
		Name:   name,
	})

	return err
}

// ListPreparedQueries lists all prepared queries for a ledger.
func ListPreparedQueries(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]*commonpb.PreparedQuery, error) {
	resp, err := client.ListPreparedQueries(ctx, &servicepb.ListPreparedQueriesRequest{
		Ledger: ledger,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetQueries(), nil
}

// ExecutePreparedQuery executes a prepared query and returns the response.
func ExecutePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, ledger, queryName string, mode commonpb.QueryMode, pageSize uint32) (*servicepb.ExecutePreparedQueryResponse, error) {
	return client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:    ledger,
		QueryName: queryName,
		Mode:      mode,
		PageSize:  pageSize,
	})
}

// ExecutePreparedQueryWithParams executes a prepared query with runtime parameters.
func ExecutePreparedQueryWithParams(ctx context.Context, client servicepb.BucketServiceClient, ledger, queryName string, mode commonpb.QueryMode, pageSize uint32, params map[string]*commonpb.ParameterValue) (*servicepb.ExecutePreparedQueryResponse, error) {
	return client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:     ledger,
		QueryName:  queryName,
		Mode:       mode,
		PageSize:   pageSize,
		Parameters: params,
	})
}
