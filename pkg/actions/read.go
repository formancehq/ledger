package actions

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// ListLedgers collects all ledgers from the streaming RPC into a map.
func ListLedgers(ctx context.Context, client servicepb.BucketServiceClient) (map[string]*commonpb.LedgerInfo, error) {
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}

	ledgers := make(map[string]*commonpb.LedgerInfo)
	for {
		ledger, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		ledgers[ledger.GetName()] = ledger
	}

	return ledgers, nil
}

// ListNumscripts collects all numscripts from the streaming RPC.
func ListNumscripts(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]*commonpb.NumscriptInfo, error) {
	stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{Ledger: ledger})
	if err != nil {
		return nil, err
	}

	var scripts []*commonpb.NumscriptInfo
	for {
		info, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		scripts = append(scripts, info)
	}

	return scripts, nil
}

// ListAllAccounts collects every account for a ledger by paginating through
// the streaming RPC. The server caps each call at MaxPageSize, so a single
// call only returns the first 1000 accounts; this helper loops with the
// last-seen address as a cursor until the server returns a short page.
func ListAllAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) ([]*commonpb.Account, error) {
	var (
		accounts     []*commonpb.Account
		afterAddress string
	)

	for {
		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
			Ledger:       ledgerName,
			PageSize:     listAllPageSize,
			AfterAddress: afterAddress,
		})
		if err != nil {
			return nil, err
		}

		page := 0

		for {
			account, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return nil, err
			}

			accounts = append(accounts, account)
			page++
		}

		if page < int(listAllPageSize) {
			return accounts, nil
		}

		afterAddress = accounts[len(accounts)-1].GetAddress()
	}
}

// ListAllTransactions collects every transaction for a ledger by paginating
// through the streaming RPC. See ListAllAccounts for the pagination shape.
func ListAllTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string) ([]*commonpb.Transaction, error) {
	var (
		transactions []*commonpb.Transaction
		afterTxID    uint64
	)

	for {
		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
			Ledger:    ledgerName,
			PageSize:  listAllPageSize,
			AfterTxId: afterTxID,
		})
		if err != nil {
			return nil, err
		}

		page := 0

		for {
			tx, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return nil, err
			}

			transactions = append(transactions, tx)
			page++
		}

		if page < int(listAllPageSize) {
			return transactions, nil
		}

		afterTxID = transactions[len(transactions)-1].GetId()
	}
}

// ListAllLogs collects every system log for a ledger by paginating through
// the streaming RPC using AfterSequence. The server caps each call at
// MaxPageSize, so we loop with the last sequence as a cursor.
func ListAllLogs(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]*commonpb.Log, error) {
	var (
		logs          []*commonpb.Log
		afterSequence uint64
	)

	for {
		req := &servicepb.ListLogsRequest{
			Ledger:   ledger,
			PageSize: listAllPageSize,
		}
		if afterSequence > 0 {
			req.AfterSequence = &afterSequence
		}

		page, err := ListLogsFiltered(ctx, client, req)
		if err != nil {
			return nil, err
		}

		logs = append(logs, page...)

		if len(page) < int(listAllPageSize) {
			return logs, nil
		}

		afterSequence = page[len(page)-1].GetSequence()
	}
}

// listAllPageSize is the per-call page size used by every ListAll* helper.
// It must match the server's MaxPageSize so each iteration of the loop
// drains a full page from the server. A short page is the loop's
// termination signal.
const listAllPageSize uint32 = 1000

// ListAllPeriods collects all periods from the streaming RPC.
func ListAllPeriods(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Period, error) {
	stream, err := client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{})
	if err != nil {
		return nil, err
	}

	var periods []*commonpb.Period
	for {
		period, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		periods = append(periods, period)
	}

	return periods, nil
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

// ListAuditEntries collects all audit entries from the streaming RPC.
func ListAuditEntries(ctx context.Context, client servicepb.BucketServiceClient, failuresOnly bool) ([]*auditpb.AuditEntry, error) {
	return ListAuditEntriesWithRequest(ctx, client, &servicepb.ListAuditEntriesRequest{
		FailuresOnly: failuresOnly,
	})
}

// ListAuditEntriesWithRequest collects all audit entries matching the given
// request by paginating through the streaming RPC. The server caps each call
// at MaxPageSize; this helper loops with the last-seen sequence as a cursor
// until the server returns a short page. The caller-supplied PageSize /
// AfterSequence on req are used to seed the first call and are then
// overwritten on each subsequent iteration.
func ListAuditEntriesWithRequest(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ListAuditEntriesRequest) ([]*auditpb.AuditEntry, error) {
	// Field-by-field copy rather than `page := *req` — protobuf-generated
	// messages embed a sync.Mutex (in MessageState) so value copy trips
	// govet (copylocks). We only need the request fields used by the
	// underlying RPC; the per-page cursor is updated below.
	page := &servicepb.ListAuditEntriesRequest{
		PageSize:      listAllPageSize,
		AfterSequence: req.AfterSequence,
		FailuresOnly:  req.GetFailuresOnly(),
		Ledger:        req.GetLedger(),
	}

	var entries []*auditpb.AuditEntry

	for {
		stream, err := client.ListAuditEntries(ctx, page)
		if err != nil {
			return nil, err
		}

		pageLen := 0

		for {
			entry, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return nil, err
			}

			entries = append(entries, entry)
			pageLen++
		}

		if pageLen < int(listAllPageSize) {
			return entries, nil
		}

		next := entries[len(entries)-1].GetSequence()
		page.AfterSequence = &next
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

// GetPeriodSchedule retrieves the current period schedule cron expression.
func GetPeriodSchedule(ctx context.Context, client servicepb.BucketServiceClient) (string, error) {
	resp, err := client.GetPeriodSchedule(ctx, &servicepb.GetPeriodScheduleRequest{})
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
		Ledger:       ledger,
		PageSize:     pageSize,
		AfterAddress: afterAddress,
		Filter:       filter,
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
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:    ledger,
		PageSize:  pageSize,
		AfterTxId: afterTxID,
		Filter:    filter,
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
		Query: &commonpb.PreparedQuery{
			Name:   name,
			Ledger: ledger,
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
