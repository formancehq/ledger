package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// mockStream implements grpc.ServerStreamingClient for testing.
type mockStream[T any] struct {
	grpc.ClientStream

	items []*T
	index int
	err   error
}

func (m *mockStream[T]) Recv() (*T, error) {
	if m.err != nil {
		return nil, m.err
	}

	if m.index >= len(m.items) {
		return nil, io.EOF
	}

	item := m.items[m.index]
	m.index++

	return item, nil
}

func (m *mockStream[T]) CloseSend() error {
	return nil
}

// mockBucketServiceClient implements servicepb.BucketServiceClient for testing.
type mockBucketServiceClient struct {
	// Simple methods - store responses/errors
	getTransactionResp        *servicepb.GetTransactionResponse
	getTransactionErr         error
	listTransactionsStream    grpc.ServerStreamingClient[commonpb.Transaction]
	listTransactionsErr       error
	getAccountResp            *commonpb.Account
	getAccountErr             error
	listAccountsStream        grpc.ServerStreamingClient[commonpb.Account]
	listAccountsErr           error
	listLogsStream            grpc.ServerStreamingClient[commonpb.Log]
	listLogsErr               error
	listLedgersStream         grpc.ServerStreamingClient[commonpb.LedgerInfo]
	listLedgersErr            error
	getLedgerResp             *commonpb.LedgerInfo
	getLedgerErr              error
	listAuditEntriesStream    grpc.ServerStreamingClient[auditpb.AuditEntry]
	listAuditEntriesErr       error
	getLogResp                *commonpb.Log
	getLogErr                 error
	getAuditEntryResp         *auditpb.AuditEntry
	getAuditEntryErr          error
	listPeriodsStream         grpc.ServerStreamingClient[commonpb.Period]
	listPeriodsErr            error
	listSigningKeysStream     grpc.ServerStreamingClient[commonpb.SigningKey]
	listSigningKeysErr        error
	getMetadataSchemaResp     *servicepb.GetMetadataSchemaStatusResponse
	getMetadataSchemaErr      error
	analyzeAccountsStream     grpc.ServerStreamingClient[servicepb.AnalyzeAccountsEvent]
	analyzeAccountsErr        error
	analyzeTransactionsStream grpc.ServerStreamingClient[servicepb.AnalyzeTransactionsEvent]
	analyzeTransactionsErr    error
	aggregateVolumesResp      *commonpb.AggregateResult
	aggregateVolumesErr       error
	listPreparedQueriesResp   *servicepb.ListPreparedQueriesResponse
	listPreparedQueriesErr    error
	executePreparedQueryResp  *servicepb.ExecutePreparedQueryResponse
	executePreparedQueryErr   error
	getLedgerStatsResp        *commonpb.LedgerStats
	getLedgerStatsErr         error
	getNumscriptResp          *commonpb.NumscriptInfo
	getNumscriptErr           error
	listNumscriptsStream      grpc.ServerStreamingClient[commonpb.NumscriptInfo]
	listNumscriptsErr         error
	applyResp                 *servicepb.ApplyResponse
	applyErr                  error

	// Capture requests for assertion
	capturedListLogsReq *servicepb.ListLogsRequest
}

func (m *mockBucketServiceClient) Apply(_ context.Context, _ *servicepb.ApplyRequest, _ ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	return m.applyResp, m.applyErr
}

func (m *mockBucketServiceClient) GetTransaction(_ context.Context, _ *servicepb.GetTransactionRequest, _ ...grpc.CallOption) (*servicepb.GetTransactionResponse, error) {
	return m.getTransactionResp, m.getTransactionErr
}

func (m *mockBucketServiceClient) ListTransactions(_ context.Context, _ *servicepb.ListTransactionsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Transaction], error) {
	return m.listTransactionsStream, m.listTransactionsErr
}

func (m *mockBucketServiceClient) GetAccount(_ context.Context, _ *servicepb.GetAccountRequest, _ ...grpc.CallOption) (*commonpb.Account, error) {
	return m.getAccountResp, m.getAccountErr
}

func (m *mockBucketServiceClient) ListAccounts(_ context.Context, _ *servicepb.ListAccountsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Account], error) {
	return m.listAccountsStream, m.listAccountsErr
}

func (m *mockBucketServiceClient) ListLogs(_ context.Context, req *servicepb.ListLogsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Log], error) {
	m.capturedListLogsReq = req

	return m.listLogsStream, m.listLogsErr
}

func (m *mockBucketServiceClient) ListLedgers(_ context.Context, _ *servicepb.ListLedgersRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.LedgerInfo], error) {
	return m.listLedgersStream, m.listLedgersErr
}

func (m *mockBucketServiceClient) GetLedger(_ context.Context, _ *servicepb.GetLedgerRequest, _ ...grpc.CallOption) (*commonpb.LedgerInfo, error) {
	return m.getLedgerResp, m.getLedgerErr
}

func (m *mockBucketServiceClient) ListAuditEntries(_ context.Context, _ *servicepb.ListAuditEntriesRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[auditpb.AuditEntry], error) {
	return m.listAuditEntriesStream, m.listAuditEntriesErr
}

func (m *mockBucketServiceClient) GetLog(_ context.Context, _ *servicepb.GetLogRequest, _ ...grpc.CallOption) (*commonpb.Log, error) {
	return m.getLogResp, m.getLogErr
}

func (m *mockBucketServiceClient) GetAuditEntry(_ context.Context, _ *servicepb.GetAuditEntryRequest, _ ...grpc.CallOption) (*auditpb.AuditEntry, error) {
	return m.getAuditEntryResp, m.getAuditEntryErr
}

func (m *mockBucketServiceClient) ListPeriods(_ context.Context, _ *servicepb.ListPeriodsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Period], error) {
	return m.listPeriodsStream, m.listPeriodsErr
}

func (m *mockBucketServiceClient) ListSigningKeys(_ context.Context, _ *servicepb.ListSigningKeysRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.SigningKey], error) {
	return m.listSigningKeysStream, m.listSigningKeysErr
}

func (m *mockBucketServiceClient) GetMetadataSchemaStatus(_ context.Context, _ *servicepb.GetMetadataSchemaStatusRequest, _ ...grpc.CallOption) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	return m.getMetadataSchemaResp, m.getMetadataSchemaErr
}

func (m *mockBucketServiceClient) AnalyzeAccounts(_ context.Context, _ *servicepb.AnalyzeAccountsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[servicepb.AnalyzeAccountsEvent], error) {
	return m.analyzeAccountsStream, m.analyzeAccountsErr
}

func (m *mockBucketServiceClient) AnalyzeTransactions(_ context.Context, _ *servicepb.AnalyzeTransactionsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[servicepb.AnalyzeTransactionsEvent], error) {
	return m.analyzeTransactionsStream, m.analyzeTransactionsErr
}

func (m *mockBucketServiceClient) AggregateVolumes(_ context.Context, _ *servicepb.AggregateVolumesRequest, _ ...grpc.CallOption) (*commonpb.AggregateResult, error) {
	return m.aggregateVolumesResp, m.aggregateVolumesErr
}

func (m *mockBucketServiceClient) ListPreparedQueries(_ context.Context, _ *servicepb.ListPreparedQueriesRequest, _ ...grpc.CallOption) (*servicepb.ListPreparedQueriesResponse, error) {
	return m.listPreparedQueriesResp, m.listPreparedQueriesErr
}

func (m *mockBucketServiceClient) ExecutePreparedQuery(_ context.Context, req *servicepb.ExecutePreparedQueryRequest, _ ...grpc.CallOption) (*servicepb.ExecutePreparedQueryResponse, error) {
	return m.executePreparedQueryResp, m.executePreparedQueryErr
}

func (m *mockBucketServiceClient) GetLedgerStats(_ context.Context, _ *servicepb.GetLedgerStatsRequest, _ ...grpc.CallOption) (*commonpb.LedgerStats, error) {
	return m.getLedgerStatsResp, m.getLedgerStatsErr
}

func (m *mockBucketServiceClient) GetNumscript(_ context.Context, _ *servicepb.GetNumscriptRequest, _ ...grpc.CallOption) (*commonpb.NumscriptInfo, error) {
	return m.getNumscriptResp, m.getNumscriptErr
}

func (m *mockBucketServiceClient) ListNumscripts(_ context.Context, _ *servicepb.ListNumscriptsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.NumscriptInfo], error) {
	return m.listNumscriptsStream, m.listNumscriptsErr
}

// Stub methods not used by BucketGrpcClient but required by the interface.
func (m *mockBucketServiceClient) GetPrimaryMetrics(_ context.Context, _ *servicepb.GetPrimaryMetricsRequest, _ ...grpc.CallOption) (*servicepb.GetPrimaryMetricsResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) GetSecondaryMetrics(_ context.Context, _ *servicepb.GetSecondaryMetricsRequest, _ ...grpc.CallOption) (*servicepb.GetSecondaryMetricsResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) CheckStore(_ context.Context, _ *servicepb.CheckStoreRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[servicepb.CheckStoreEvent], error) {
	return nil, nil
}

func (m *mockBucketServiceClient) GetEventsSinks(_ context.Context, _ *servicepb.GetEventsSinksRequest, _ ...grpc.CallOption) (*servicepb.GetEventsSinksResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) GetPeriodSchedule(_ context.Context, _ *servicepb.GetPeriodScheduleRequest, _ ...grpc.CallOption) (*servicepb.GetPeriodScheduleResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) Discovery(_ context.Context, _ *servicepb.DiscoveryRequest, _ ...grpc.CallOption) (*servicepb.DiscoveryResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) CreatePreparedQuery(_ context.Context, _ *servicepb.CreatePreparedQueryRequest, _ ...grpc.CallOption) (*servicepb.CreatePreparedQueryResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) UpdatePreparedQuery(_ context.Context, _ *servicepb.UpdatePreparedQueryRequest, _ ...grpc.CallOption) (*servicepb.UpdatePreparedQueryResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) DeletePreparedQuery(_ context.Context, _ *servicepb.DeletePreparedQueryRequest, _ ...grpc.CallOption) (*servicepb.DeletePreparedQueryResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) GetIndexStatus(_ context.Context, _ *servicepb.GetIndexStatusRequest, _ ...grpc.CallOption) (*servicepb.GetIndexStatusResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) Barrier(_ context.Context, _ *servicepb.BarrierRequest, _ ...grpc.CallOption) (*servicepb.BarrierResponse, error) {
	return nil, nil
}

func (m *mockBucketServiceClient) InspectIndex(_ context.Context, _ *servicepb.InspectIndexRequest, _ ...grpc.CallOption) (*servicepb.InspectIndexResponse, error) {
	return nil, nil
}

var _ servicepb.BucketServiceClient = (*mockBucketServiceClient)(nil)

// --- Tests ---

func TestGetTransaction_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.Transaction{Id: 42}
	mock := &mockBucketServiceClient{
		getTransactionResp: &servicepb.GetTransactionResponse{Transaction: expected},
	}

	client := NewLedgerGrpcClient(mock)
	tx, err := client.GetTransaction(context.Background(), "ledger1", 42)
	require.NoError(t, err)
	require.Equal(t, uint64(42), tx.GetId())
}

func TestGetTransaction_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		getTransactionErr: errors.New("not found"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetTransaction(context.Background(), "ledger1", 99)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestListTransactions_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.Transaction]{
		items: []*commonpb.Transaction{
			{Id: 1},
			{Id: 2},
		},
	}
	mock := &mockBucketServiceClient{listTransactionsStream: stream}

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListTransactions(context.Background(), "ledger1", 10, 0, nil, false)
	require.NoError(t, err)

	tx1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), tx1.GetId())

	tx2, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(2), tx2.GetId())

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestListTransactions_StreamError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listTransactionsErr: errors.New("stream init failed"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListTransactions(context.Background(), "ledger1", 10, 0, nil, false)
	require.Error(t, err)
}

func TestGetAccount_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.Account{Address: "user:001"}
	mock := &mockBucketServiceClient{getAccountResp: expected}

	client := NewLedgerGrpcClient(mock)
	account, err := client.GetAccount(context.Background(), "ledger1", "user:001")
	require.NoError(t, err)
	require.Equal(t, "user:001", account.GetAddress())
}

func TestGetAccount_ReturnsError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{getAccountErr: errors.New("unavailable")}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetAccount(context.Background(), "ledger1", "user:001")
	require.Error(t, err)
}

func TestListAccounts_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.Account]{
		items: []*commonpb.Account{
			{Address: "user:001"},
			{Address: "user:002"},
		},
	}
	mock := &mockBucketServiceClient{listAccountsStream: stream}

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListAccounts(context.Background(), "ledger1", 10, "", nil, false)
	require.NoError(t, err)

	acc1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "user:001", acc1.GetAddress())

	acc2, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "user:002", acc2.GetAddress())
}

func TestListAccounts_StreamError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listAccountsErr: errors.New("stream error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListAccounts(context.Background(), "ledger1", 10, "", nil, false)
	require.Error(t, err)
}

func TestListLogs_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.Log]{
		items: []*commonpb.Log{
			{Sequence: 1},
			{Sequence: 2},
		},
	}
	mock := &mockBucketServiceClient{listLogsStream: stream}

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListLogs(context.Background(), 0, 10, nil)
	require.NoError(t, err)

	log1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), log1.GetSequence())

	// Verify afterSequence is not set when 0
	require.Nil(t, mock.capturedListLogsReq.AfterSequence)
}

func TestListLogs_WithAfterSequence(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.Log]{
		items: []*commonpb.Log{{Sequence: 5}},
	}
	mock := &mockBucketServiceClient{listLogsStream: stream}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListLogs(context.Background(), 3, 10, nil)
	require.NoError(t, err)

	// Verify afterSequence is set when > 0
	require.NotNil(t, mock.capturedListLogsReq.AfterSequence)
	require.Equal(t, uint64(3), mock.capturedListLogsReq.GetAfterSequence())
}

func TestListLogs_StreamError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listLogsErr: errors.New("list logs failed"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListLogs(context.Background(), 0, 10, nil)
	require.Error(t, err)
}

func TestListLedgers_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.LedgerInfo]{
		items: []*commonpb.LedgerInfo{
			{Name: "ledger1"},
			{Name: "ledger2"},
		},
	}
	mock := &mockBucketServiceClient{listLedgersStream: stream}

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListLedgers(context.Background())
	require.NoError(t, err)

	info1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "ledger1", info1.GetName())
}

func TestListLedgers_StreamError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listLedgersErr: errors.New("list ledgers failed"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListLedgers(context.Background())
	require.Error(t, err)
}

func TestGetLedgerByName_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.LedgerInfo{Name: "my-ledger"}
	mock := &mockBucketServiceClient{getLedgerResp: expected}

	client := NewLedgerGrpcClient(mock)
	info, err := client.GetLedgerByName(context.Background(), "my-ledger")
	require.NoError(t, err)
	require.Equal(t, "my-ledger", info.GetName())
}

func TestGetLedgerByName_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		getLedgerErr: errors.New("not found"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetLedgerByName(context.Background(), "missing")
	require.Error(t, err)
}

func TestListAuditEntries_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[auditpb.AuditEntry]{
		items: []*auditpb.AuditEntry{
			{Sequence: 1},
		},
	}
	mock := &mockBucketServiceClient{listAuditEntriesStream: stream}

	client := NewLedgerGrpcClient(mock)
	seq := uint64(5)
	cursor, err := client.ListAuditEntries(context.Background(), &seq, true, 10, "")
	require.NoError(t, err)

	entry, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), entry.GetSequence())
}

func TestListAuditEntries_StreamError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listAuditEntriesErr: errors.New("audit error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListAuditEntries(context.Background(), nil, false, 10, "")
	require.Error(t, err)
}

func TestGetLog_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.Log{Sequence: 42}
	mock := &mockBucketServiceClient{getLogResp: expected}

	client := NewLedgerGrpcClient(mock)
	log, err := client.GetLog(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, uint64(42), log.GetSequence())
}

func TestGetLog_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		getLogErr: errors.New("log not found"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetLog(context.Background(), 99)
	require.Error(t, err)
}

func TestGetAuditEntry_Success(t *testing.T) {
	t.Parallel()

	expected := &auditpb.AuditEntry{Sequence: 7}
	mock := &mockBucketServiceClient{getAuditEntryResp: expected}

	client := NewLedgerGrpcClient(mock)
	entry, err := client.GetAuditEntry(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, uint64(7), entry.GetSequence())
}

func TestGetAuditEntry_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		getAuditEntryErr: errors.New("audit entry not found"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetAuditEntry(context.Background(), 99)
	require.Error(t, err)
}

func TestListPeriods_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.Period]{
		items: []*commonpb.Period{
			{Id: 1},
		},
	}
	mock := &mockBucketServiceClient{listPeriodsStream: stream}

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListPeriods(context.Background())
	require.NoError(t, err)

	period, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), period.GetId())
}

func TestListPeriods_StreamError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listPeriodsErr: errors.New("periods error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListPeriods(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC ListPeriods call failed")
}

func TestListSigningKeys_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.SigningKey]{
		items: []*commonpb.SigningKey{
			{KeyId: "key-1"},
		},
	}
	mock := &mockBucketServiceClient{listSigningKeysStream: stream}

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListSigningKeys(context.Background())
	require.NoError(t, err)

	key, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "key-1", key.GetKeyId())
}

func TestListSigningKeys_StreamError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listSigningKeysErr: errors.New("keys error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListSigningKeys(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC ListSigningKeys call failed")
}

func TestGetMetadataSchemaStatus_Success(t *testing.T) {
	t.Parallel()

	expected := &servicepb.GetMetadataSchemaStatusResponse{}
	mock := &mockBucketServiceClient{getMetadataSchemaResp: expected}

	client := NewLedgerGrpcClient(mock)
	resp, err := client.GetMetadataSchemaStatus(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Equal(t, expected, resp)
}

func TestGetMetadataSchemaStatus_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		getMetadataSchemaErr: errors.New("schema error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetMetadataSchemaStatus(context.Background(), "ledger1")
	require.Error(t, err)
}

func TestAnalyzeAccounts_ProgressThenResult(t *testing.T) {
	t.Parallel()

	expected := &servicepb.AnalyzeAccountsResponse{}
	stream := &mockStream[servicepb.AnalyzeAccountsEvent]{
		items: []*servicepb.AnalyzeAccountsEvent{
			{Type: &servicepb.AnalyzeAccountsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{Processed: 10, Total: 100},
			}},
			{Type: &servicepb.AnalyzeAccountsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{Processed: 50, Total: 100},
			}},
			{Type: &servicepb.AnalyzeAccountsEvent_Result{
				Result: expected,
			}},
		},
	}
	mock := &mockBucketServiceClient{analyzeAccountsStream: stream}

	var progressCalls []struct{ processed, total uint64 }

	client := NewLedgerGrpcClient(mock)
	result, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, func(processed, total uint64) {
		progressCalls = append(progressCalls, struct{ processed, total uint64 }{processed, total})
	})
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.Len(t, progressCalls, 2)
	require.Equal(t, uint64(10), progressCalls[0].processed)
	require.Equal(t, uint64(50), progressCalls[1].processed)
}

func TestAnalyzeAccounts_NilProgressCallback(t *testing.T) {
	t.Parallel()

	expected := &servicepb.AnalyzeAccountsResponse{}
	stream := &mockStream[servicepb.AnalyzeAccountsEvent]{
		items: []*servicepb.AnalyzeAccountsEvent{
			{Type: &servicepb.AnalyzeAccountsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{Processed: 10, Total: 100},
			}},
			{Type: &servicepb.AnalyzeAccountsEvent_Result{
				Result: expected,
			}},
		},
	}
	mock := &mockBucketServiceClient{analyzeAccountsStream: stream}

	client := NewLedgerGrpcClient(mock)
	result, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestAnalyzeAccounts_StreamInitError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		analyzeAccountsErr: errors.New("stream init error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC AnalyzeAccounts stream")
}

func TestAnalyzeAccounts_StreamRecvError(t *testing.T) {
	t.Parallel()

	stream := &mockStream[servicepb.AnalyzeAccountsEvent]{
		err: errors.New("recv error"),
	}
	mock := &mockBucketServiceClient{analyzeAccountsStream: stream}

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "receiving AnalyzeAccounts event")
}

func TestAnalyzeAccounts_EOFWithoutResult(t *testing.T) {
	t.Parallel()

	// Empty stream - will return EOF immediately
	stream := &mockStream[servicepb.AnalyzeAccountsEvent]{
		items: []*servicepb.AnalyzeAccountsEvent{},
	}
	mock := &mockBucketServiceClient{analyzeAccountsStream: stream}

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream ended without result")
}

func TestAnalyzeTransactions_ProgressThenResult(t *testing.T) {
	t.Parallel()

	expected := &servicepb.AnalyzeTransactionsResponse{}
	stream := &mockStream[servicepb.AnalyzeTransactionsEvent]{
		items: []*servicepb.AnalyzeTransactionsEvent{
			{Type: &servicepb.AnalyzeTransactionsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{Processed: 25, Total: 200},
			}},
			{Type: &servicepb.AnalyzeTransactionsEvent_Result{
				Result: expected,
			}},
		},
	}
	mock := &mockBucketServiceClient{analyzeTransactionsStream: stream}

	var progressCalls []struct{ processed, total uint64 }

	client := NewLedgerGrpcClient(mock)
	result, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, func(processed, total uint64) {
		progressCalls = append(progressCalls, struct{ processed, total uint64 }{processed, total})
	})
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.Len(t, progressCalls, 1)
	require.Equal(t, uint64(25), progressCalls[0].processed)
}

func TestAnalyzeTransactions_NilProgressCallback(t *testing.T) {
	t.Parallel()

	expected := &servicepb.AnalyzeTransactionsResponse{}
	stream := &mockStream[servicepb.AnalyzeTransactionsEvent]{
		items: []*servicepb.AnalyzeTransactionsEvent{
			{Type: &servicepb.AnalyzeTransactionsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{Processed: 10, Total: 100},
			}},
			{Type: &servicepb.AnalyzeTransactionsEvent_Result{
				Result: expected,
			}},
		},
	}
	mock := &mockBucketServiceClient{analyzeTransactionsStream: stream}

	client := NewLedgerGrpcClient(mock)
	result, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestAnalyzeTransactions_StreamInitError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		analyzeTransactionsErr: errors.New("stream init error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC AnalyzeTransactions stream")
}

func TestAnalyzeTransactions_StreamRecvError(t *testing.T) {
	t.Parallel()

	stream := &mockStream[servicepb.AnalyzeTransactionsEvent]{
		err: errors.New("recv error"),
	}
	mock := &mockBucketServiceClient{analyzeTransactionsStream: stream}

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "receiving AnalyzeTransactions event")
}

func TestAnalyzeTransactions_EOFWithoutResult(t *testing.T) {
	t.Parallel()

	stream := &mockStream[servicepb.AnalyzeTransactionsEvent]{
		items: []*servicepb.AnalyzeTransactionsEvent{},
	}
	mock := &mockBucketServiceClient{analyzeTransactionsStream: stream}

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream ended without result")
}

func TestAggregateVolumes_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.AggregateResult{}
	mock := &mockBucketServiceClient{aggregateVolumesResp: expected}

	client := NewLedgerGrpcClient(mock)
	result, err := client.AggregateVolumes(context.Background(), "ledger1", nil, query.AggregateOptions{
		UseMaxPrecision: true,
		GroupByPrefixes: []string{"user:"},
	})
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestAggregateVolumes_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		aggregateVolumesErr: errors.New("aggregate error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.AggregateVolumes(context.Background(), "ledger1", nil, query.AggregateOptions{})
	require.Error(t, err)
}

func TestListPreparedQueries_Success(t *testing.T) {
	t.Parallel()

	queries := []*commonpb.PreparedQuery{
		{Name: "q1"},
		{Name: "q2"},
	}
	mock := &mockBucketServiceClient{
		listPreparedQueriesResp: &servicepb.ListPreparedQueriesResponse{Queries: queries},
	}

	client := NewLedgerGrpcClient(mock)
	result, err := client.ListPreparedQueries(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, "q1", result[0].GetName())
}

func TestListPreparedQueries_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listPreparedQueriesErr: errors.New("query error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListPreparedQueries(context.Background(), "ledger1")
	require.Error(t, err)
}

func TestExecutePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	expected := &servicepb.ExecutePreparedQueryResponse{}
	mock := &mockBucketServiceClient{executePreparedQueryResp: expected}

	client := NewLedgerGrpcClient(mock)
	result, err := client.ExecutePreparedQuery(context.Background(), &servicepb.ExecutePreparedQueryRequest{
		Ledger:    "ledger1",
		QueryName: "q1",
	})
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestExecutePreparedQuery_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		executePreparedQueryErr: errors.New("exec error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ExecutePreparedQuery(context.Background(), &servicepb.ExecutePreparedQueryRequest{})
	require.Error(t, err)
}

func TestGetLedgerStats_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.LedgerStats{}
	mock := &mockBucketServiceClient{getLedgerStatsResp: expected}

	client := NewLedgerGrpcClient(mock)
	result, err := client.GetLedgerStats(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestGetLedgerStats_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		getLedgerStatsErr: errors.New("stats error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetLedgerStats(context.Background(), "ledger1")
	require.Error(t, err)
}

func TestGetNumscript_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.NumscriptInfo{Name: "my-script"}
	mock := &mockBucketServiceClient{getNumscriptResp: expected}

	client := NewLedgerGrpcClient(mock)
	result, err := client.GetNumscript(context.Background(), "ledger1", "my-script", "v1")
	require.NoError(t, err)
	require.Equal(t, "my-script", result.GetName())
}

func TestGetNumscript_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		getNumscriptErr: errors.New("numscript not found"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetNumscript(context.Background(), "ledger1", "missing", "v1")
	require.Error(t, err)
}

func TestListNumscripts_Success(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.NumscriptInfo]{
		items: []*commonpb.NumscriptInfo{
			{Name: "script1"},
			{Name: "script2"},
		},
	}
	mock := &mockBucketServiceClient{listNumscriptsStream: stream}

	client := NewLedgerGrpcClient(mock)
	scripts, err := client.ListNumscripts(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Len(t, scripts, 2)
	require.Equal(t, "script1", scripts[0].GetName())
	require.Equal(t, "script2", scripts[1].GetName())
}

func TestListNumscripts_StreamInitError(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		listNumscriptsErr: errors.New("stream error"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListNumscripts(context.Background(), "ledger1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC ListNumscripts call failed")
}

func TestListNumscripts_StreamRecvError(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.NumscriptInfo]{
		err: errors.New("recv failed"),
	}
	mock := &mockBucketServiceClient{listNumscriptsStream: stream}

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListNumscripts(context.Background(), "ledger1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "receiving numscript")
}

func TestListNumscripts_EmptyStream(t *testing.T) {
	t.Parallel()

	stream := &mockStream[commonpb.NumscriptInfo]{
		items: []*commonpb.NumscriptInfo{},
	}
	mock := &mockBucketServiceClient{listNumscriptsStream: stream}

	client := NewLedgerGrpcClient(mock)
	scripts, err := client.ListNumscripts(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Empty(t, scripts)
}

func TestApply_Success(t *testing.T) {
	t.Parallel()

	logs := []*commonpb.Log{{Sequence: 1}}
	mock := &mockBucketServiceClient{
		applyResp: &servicepb.ApplyResponse{Logs: logs},
	}

	client := NewLedgerGrpcClient(mock)
	result, err := client.Apply(context.Background(), &servicepb.Request{})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, uint64(1), result[0].GetSequence())
}

func TestApply_Error(t *testing.T) {
	t.Parallel()

	mock := &mockBucketServiceClient{
		applyErr: errors.New("apply failed"),
	}

	client := NewLedgerGrpcClient(mock)
	_, err := client.Apply(context.Background(), &servicepb.Request{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC call failed")
}
