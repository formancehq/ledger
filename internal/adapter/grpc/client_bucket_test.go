package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// newRecvStream returns a generated mock ServerStreamingClient[T] that yields
// each item from items in order then io.EOF. If recvErr is non-nil, Recv
// returns (nil, recvErr) on every call instead. Trailer() returns an empty
// metadata.MD by default so the cursor-pagination helpers in
// BucketGrpcClient see "no more pages" rather than panicking.
func newRecvStream[T any](ctrl *gomock.Controller, items []*T, recvErr error) *MockServerStreamingClient[T] {
	stream := NewMockServerStreamingClient[T](ctrl)
	stream.EXPECT().Trailer().Return(metadata.MD{}).AnyTimes()
	if recvErr != nil {
		stream.EXPECT().Recv().DoAndReturn(func() (*T, error) {
			return nil, recvErr
		}).AnyTimes()

		return stream
	}
	idx := 0
	stream.EXPECT().Recv().DoAndReturn(func() (*T, error) {
		if idx >= len(items) {
			return nil, io.EOF
		}
		item := items[idx]
		idx++

		return item, nil
	}).AnyTimes()

	return stream
}

// --- Tests ---

func TestGetTransaction_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.Transaction{Id: 42}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetTransaction(gomock.Any(), gomock.Any()).Return(
		&servicepb.GetTransactionResponse{Transaction: expected, Receipt: "leader-receipt"}, nil,
	)

	client := NewLedgerGrpcClient(mock)
	tx, receipt, err := client.GetTransaction(context.Background(), "ledger1", 42)
	require.NoError(t, err)
	require.Equal(t, uint64(42), tx.GetId())
	// The receipt the serving node signed must be surfaced, not discarded.
	require.Equal(t, "leader-receipt", receipt)
}

func TestGetTransaction_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetTransaction(gomock.Any(), gomock.Any()).Return(nil, errors.New("not found"))

	client := NewLedgerGrpcClient(mock)
	_, _, err := client.GetTransaction(context.Background(), "ledger1", 99)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestListTransactions_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.Transaction](ctrl, []*commonpb.Transaction{
		{Id: 1},
		{Id: 2},
	}, nil)
	mock.EXPECT().ListTransactions(gomock.Any(), gomock.Any()).Return(stream, nil)

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

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListTransactions(gomock.Any(), gomock.Any()).Return(nil, errors.New("stream init failed"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListTransactions(context.Background(), "ledger1", 10, 0, nil, false)
	require.Error(t, err)
}

func TestGetAccount_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.Account{Address: "user:001"}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetAccount(gomock.Any(), gomock.Any()).Return(expected, nil)

	client := NewLedgerGrpcClient(mock)
	account, err := client.GetAccount(context.Background(), "ledger1", "user:001")
	require.NoError(t, err)
	require.Equal(t, "user:001", account.GetAddress())
}

func TestGetAccount_ReturnsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetAccount(gomock.Any(), gomock.Any()).Return(nil, errors.New("unavailable"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetAccount(context.Background(), "ledger1", "user:001")
	require.Error(t, err)
}

func TestListAccounts_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.Account](ctrl, []*commonpb.Account{
		{Address: "user:001"},
		{Address: "user:002"},
	}, nil)
	mock.EXPECT().ListAccounts(gomock.Any(), gomock.Any()).Return(stream, nil)

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

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListAccounts(gomock.Any(), gomock.Any()).Return(nil, errors.New("stream error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListAccounts(context.Background(), "ledger1", 10, "", nil, false)
	require.Error(t, err)
}

func TestListLogs_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.Log](ctrl, []*commonpb.Log{
		{Sequence: 1},
		{Sequence: 2},
	}, nil)
	var capturedListLogsReq *servicepb.ListLogsRequest
	mock.EXPECT().ListLogs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ListLogsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Log], error) {
			capturedListLogsReq = req

			return stream, nil
		})

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListLogs(context.Background(), "ledger1", 0, 10, nil)
	require.NoError(t, err)

	log1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), log1.GetSequence())

	// Verify cursor is not set when afterSequence is 0
	require.Empty(t, capturedListLogsReq.GetOptions().GetCursor())
	// Verify ledger is set
	require.Equal(t, "ledger1", capturedListLogsReq.GetLedger())
}

func TestListLogs_WithAfterSequence(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.Log](ctrl, []*commonpb.Log{{Sequence: 5}}, nil)
	var capturedListLogsReq *servicepb.ListLogsRequest
	mock.EXPECT().ListLogs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ListLogsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Log], error) {
			capturedListLogsReq = req

			return stream, nil
		})

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListLogs(context.Background(), "ledger1", 3, 10, nil)
	require.NoError(t, err)

	// The client converts the typed afterSequence into the opaque ListOptions.cursor
	// (decimal-encoded uint64) — server decodes it back.
	require.Equal(t, "3", capturedListLogsReq.GetOptions().GetCursor())
}

func TestListLogs_StreamError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListLogs(gomock.Any(), gomock.Any()).Return(nil, errors.New("list logs failed"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListLogs(context.Background(), "ledger1", 0, 10, nil)
	require.Error(t, err)
}

func TestListLedgers_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.LedgerInfo](ctrl, []*commonpb.LedgerInfo{
		{Name: "ledger1"},
		{Name: "ledger2"},
	}, nil)
	mock.EXPECT().ListLedgers(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListLedgers(context.Background())
	require.NoError(t, err)

	info1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "ledger1", info1.GetName())
}

func TestListLedgers_StreamError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListLedgers(gomock.Any(), gomock.Any()).Return(nil, errors.New("list ledgers failed"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListLedgers(context.Background())
	require.Error(t, err)
}

func TestGetLedgerByName_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.LedgerInfo{Name: "my-ledger"}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetLedger(gomock.Any(), gomock.Any()).Return(expected, nil)

	client := NewLedgerGrpcClient(mock)
	info, err := client.GetLedgerByName(context.Background(), "my-ledger")
	require.NoError(t, err)
	require.Equal(t, "my-ledger", info.GetName())
}

func TestGetLedgerByName_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetLedger(gomock.Any(), gomock.Any()).Return(nil, errors.New("not found"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetLedgerByName(context.Background(), "missing")
	require.Error(t, err)
}

func TestListAuditEntries_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[auditpb.AuditEntry](ctrl, []*auditpb.AuditEntry{
		{Sequence: 1},
	}, nil)
	mock.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any()).Return(stream, nil)

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

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any()).Return(nil, errors.New("audit error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListAuditEntries(context.Background(), nil, false, 10, "")
	require.Error(t, err)
}

func TestGetLog_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.Log{Sequence: 42}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetLog(gomock.Any(), gomock.Any()).Return(expected, nil)

	client := NewLedgerGrpcClient(mock)
	log, err := client.GetLog(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, uint64(42), log.GetSequence())
}

func TestGetLog_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetLog(gomock.Any(), gomock.Any()).Return(nil, errors.New("log not found"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetLog(context.Background(), 99)
	require.Error(t, err)
}

func TestGetAuditEntry_Success(t *testing.T) {
	t.Parallel()

	expected := &auditpb.AuditEntry{Sequence: 7}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetAuditEntry(gomock.Any(), gomock.Any()).Return(expected, nil)

	client := NewLedgerGrpcClient(mock)
	entry, err := client.GetAuditEntry(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, uint64(7), entry.GetSequence())
}

func TestGetAuditEntry_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetAuditEntry(gomock.Any(), gomock.Any()).Return(nil, errors.New("audit entry not found"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetAuditEntry(context.Background(), 99)
	require.Error(t, err)
}

func TestListChapters_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.Chapter](ctrl, []*commonpb.Chapter{
		{Id: 1},
	}, nil)
	mock.EXPECT().ListChapters(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListChapters(context.Background())
	require.NoError(t, err)

	chapter, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), chapter.GetId())
}

func TestListChapters_StreamError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListChapters(gomock.Any(), gomock.Any()).Return(nil, errors.New("chapters error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListChapters(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC ListChapters call failed")
}

func TestListSigningKeys_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.SigningKey](ctrl, []*commonpb.SigningKey{
		{KeyId: "key-1"},
	}, nil)
	mock.EXPECT().ListSigningKeys(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	cursor, err := client.ListSigningKeys(context.Background())
	require.NoError(t, err)

	key, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "key-1", key.GetKeyId())
}

func TestListSigningKeys_StreamError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListSigningKeys(gomock.Any(), gomock.Any()).Return(nil, errors.New("keys error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListSigningKeys(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC ListSigningKeys call failed")
}

func TestGetMetadataSchemaStatus_Success(t *testing.T) {
	t.Parallel()

	expected := &servicepb.GetMetadataSchemaStatusResponse{}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetMetadataSchemaStatus(gomock.Any(), gomock.Any()).Return(expected, nil)

	client := NewLedgerGrpcClient(mock)
	resp, err := client.GetMetadataSchemaStatus(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Equal(t, expected, resp)
}

func TestGetMetadataSchemaStatus_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetMetadataSchemaStatus(gomock.Any(), gomock.Any()).Return(nil, errors.New("schema error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetMetadataSchemaStatus(context.Background(), "ledger1")
	require.Error(t, err)
}

func TestAnalyzeAccounts_ProgressThenResult(t *testing.T) {
	t.Parallel()

	expected := &servicepb.AnalyzeAccountsResponse{}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[servicepb.AnalyzeAccountsEvent](ctrl, []*servicepb.AnalyzeAccountsEvent{
		{Type: &servicepb.AnalyzeAccountsEvent_Progress{
			Progress: &servicepb.AnalyzeProgress{Processed: 10, Total: 100},
		}},
		{Type: &servicepb.AnalyzeAccountsEvent_Progress{
			Progress: &servicepb.AnalyzeProgress{Processed: 50, Total: 100},
		}},
		{Type: &servicepb.AnalyzeAccountsEvent_Result{
			Result: expected,
		}},
	}, nil)
	mock.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any()).Return(stream, nil)

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
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[servicepb.AnalyzeAccountsEvent](ctrl, []*servicepb.AnalyzeAccountsEvent{
		{Type: &servicepb.AnalyzeAccountsEvent_Progress{
			Progress: &servicepb.AnalyzeProgress{Processed: 10, Total: 100},
		}},
		{Type: &servicepb.AnalyzeAccountsEvent_Result{
			Result: expected,
		}},
	}, nil)
	mock.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	result, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestAnalyzeAccounts_StreamInitError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any()).Return(nil, errors.New("stream init error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC AnalyzeAccounts stream")
}

func TestAnalyzeAccounts_StreamRecvError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[servicepb.AnalyzeAccountsEvent](ctrl, nil, errors.New("recv error"))
	mock.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "receiving AnalyzeAccounts event")
}

func TestAnalyzeAccounts_EOFWithoutResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	// Empty stream - will return EOF immediately
	stream := newRecvStream[servicepb.AnalyzeAccountsEvent](ctrl, nil, nil)
	mock.EXPECT().AnalyzeAccounts(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeAccounts(context.Background(), "ledger1", 5, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream ended without result")
}

func TestAnalyzeTransactions_ProgressThenResult(t *testing.T) {
	t.Parallel()

	expected := &servicepb.AnalyzeTransactionsResponse{}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[servicepb.AnalyzeTransactionsEvent](ctrl, []*servicepb.AnalyzeTransactionsEvent{
		{Type: &servicepb.AnalyzeTransactionsEvent_Progress{
			Progress: &servicepb.AnalyzeProgress{Processed: 25, Total: 200},
		}},
		{Type: &servicepb.AnalyzeTransactionsEvent_Result{
			Result: expected,
		}},
	}, nil)
	mock.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any()).Return(stream, nil)

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
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[servicepb.AnalyzeTransactionsEvent](ctrl, []*servicepb.AnalyzeTransactionsEvent{
		{Type: &servicepb.AnalyzeTransactionsEvent_Progress{
			Progress: &servicepb.AnalyzeProgress{Processed: 10, Total: 100},
		}},
		{Type: &servicepb.AnalyzeTransactionsEvent_Result{
			Result: expected,
		}},
	}, nil)
	mock.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	result, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestAnalyzeTransactions_StreamInitError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any()).Return(nil, errors.New("stream init error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC AnalyzeTransactions stream")
}

func TestAnalyzeTransactions_StreamRecvError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[servicepb.AnalyzeTransactionsEvent](ctrl, nil, errors.New("recv error"))
	mock.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "receiving AnalyzeTransactions event")
}

func TestAnalyzeTransactions_EOFWithoutResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[servicepb.AnalyzeTransactionsEvent](ctrl, nil, nil)
	mock.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	_, err := client.AnalyzeTransactions(context.Background(), "ledger1", 3, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream ended without result")
}

func TestAggregateVolumes_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.AggregateResult{}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any()).Return(expected, nil)

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

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any()).Return(nil, errors.New("aggregate error"))

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
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListPreparedQueries(gomock.Any(), gomock.Any()).Return(
		&servicepb.ListPreparedQueriesResponse{Queries: queries}, nil,
	)

	client := NewLedgerGrpcClient(mock)
	result, err := client.ListPreparedQueries(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, "q1", result[0].GetName())
}

func TestListPreparedQueries_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListPreparedQueries(gomock.Any(), gomock.Any()).Return(nil, errors.New("query error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListPreparedQueries(context.Background(), "ledger1")
	require.Error(t, err)
}

func TestExecutePreparedQuery_Success(t *testing.T) {
	t.Parallel()

	expected := &servicepb.ExecutePreparedQueryResponse{}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).Return(expected, nil)

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

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).Return(nil, errors.New("exec error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ExecutePreparedQuery(context.Background(), &servicepb.ExecutePreparedQueryRequest{})
	require.Error(t, err)
}

func TestGetLedgerStats_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.LedgerStats{}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetLedgerStats(gomock.Any(), gomock.Any()).Return(expected, nil)

	client := NewLedgerGrpcClient(mock)
	result, err := client.GetLedgerStats(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestGetLedgerStats_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetLedgerStats(gomock.Any(), gomock.Any()).Return(nil, errors.New("stats error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetLedgerStats(context.Background(), "ledger1")
	require.Error(t, err)
}

func TestGetNumscript_Success(t *testing.T) {
	t.Parallel()

	expected := &commonpb.NumscriptInfo{Name: "my-script"}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetNumscript(gomock.Any(), gomock.Any()).Return(expected, nil)

	client := NewLedgerGrpcClient(mock)
	result, err := client.GetNumscript(context.Background(), "ledger1", "my-script", "v1")
	require.NoError(t, err)
	require.Equal(t, "my-script", result.GetName())
}

func TestGetNumscript_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().GetNumscript(gomock.Any(), gomock.Any()).Return(nil, errors.New("numscript not found"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.GetNumscript(context.Background(), "ledger1", "missing", "v1")
	require.Error(t, err)
}

func TestListNumscripts_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.NumscriptInfo](ctrl, []*commonpb.NumscriptInfo{
		{Name: "script1"},
		{Name: "script2"},
	}, nil)
	mock.EXPECT().ListNumscripts(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	scripts, err := client.ListNumscripts(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Len(t, scripts, 2)
	require.Equal(t, "script1", scripts[0].GetName())
	require.Equal(t, "script2", scripts[1].GetName())
}

func TestListNumscripts_StreamInitError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().ListNumscripts(gomock.Any(), gomock.Any()).Return(nil, errors.New("stream error"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListNumscripts(context.Background(), "ledger1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC ListNumscripts call failed")
}

func TestListNumscripts_StreamRecvError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.NumscriptInfo](ctrl, nil, errors.New("recv failed"))
	mock.EXPECT().ListNumscripts(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	_, err := client.ListNumscripts(context.Background(), "ledger1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "receiving numscript")
}

func TestListNumscripts_EmptyStream(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	stream := newRecvStream[commonpb.NumscriptInfo](ctrl, nil, nil)
	mock.EXPECT().ListNumscripts(gomock.Any(), gomock.Any()).Return(stream, nil)

	client := NewLedgerGrpcClient(mock)
	scripts, err := client.ListNumscripts(context.Background(), "ledger1")
	require.NoError(t, err)
	require.Empty(t, scripts)
}

func TestApply_Success(t *testing.T) {
	t.Parallel()

	logs := []*commonpb.Log{{Sequence: 1}}
	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(
		&servicepb.ApplyResponse{Logs: logs}, nil,
	)

	client := NewLedgerGrpcClient(mock)
	result, err := client.Apply(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{}))
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, uint64(1), result[0].GetSequence())
}

func TestApply_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	mock.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, errors.New("apply failed"))

	client := NewLedgerGrpcClient(mock)
	_, err := client.Apply(context.Background(), servicepb.UnsignedApplyRequest("", &servicepb.Request{}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "gRPC call failed")
}

// TestApply_ForwardsCallerSnapshot verifies that when a follower forwards an
// Apply to the leader, it captures the admission-time caller snapshot
// (identity + scopes + god) from the local context and includes it on the
// wire so the leader can attribute the audit entry to the original user
// despite the cluster-secret hop.
//
// Regression test for #362 / EN-1079.
func TestApply_ForwardsCallerSnapshot(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	var capturedApplyReq *servicepb.ApplyRequest
	mock.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest, _ ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
			capturedApplyReq = req

			return &servicepb.ApplyResponse{}, nil
		})

	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{
			Subject: "alice",
			Issuer:  "https://idp.example.com",
		},
	}
	ctx := auth.WithClaims(context.Background(), claims)

	client := NewLedgerGrpcClient(mock)
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{}))
	require.NoError(t, err)

	fc := capturedApplyReq.GetForwardedCallerSnapshot()
	require.NotNil(t, fc, "follower must forward the caller snapshot")
	require.Equal(t, "alice", fc.GetIdentity().GetSubject())
	require.Equal(t, "https://idp.example.com", fc.GetIdentity().GetIssuer())
}

// TestApply_PropagatesExistingForwardedSnapshot verifies that a node
// receiving an Apply already carrying a forwarded_caller (multi-hop forward)
// preserves the original snapshot rather than overwriting it with its own
// claims.
func TestApply_PropagatesExistingForwardedSnapshot(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockBucketServiceClient(ctrl)
	var capturedApplyReq *servicepb.ApplyRequest
	mock.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest, _ ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
			capturedApplyReq = req

			return &servicepb.ApplyResponse{}, nil
		})

	// Simulate a node that received the request via cluster-internal forward.
	original := &commonpb.CallerSnapshot{
		Identity: &commonpb.CallerIdentity{
			Subject: "original-user",
			Source:  &commonpb.CallerIdentity_KeyId{KeyId: "ed25519-7"},
		},
		Scopes: []string{"transactions:write"},
	}
	ctx := auth.WithForwardedSnapshot(context.Background(), original)

	client := NewLedgerGrpcClient(mock)
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{}))
	require.NoError(t, err)

	fc := capturedApplyReq.GetForwardedCallerSnapshot()
	require.NotNil(t, fc)
	require.Equal(t, "original-user", fc.GetIdentity().GetSubject())
	require.Equal(t, "ed25519-7", fc.GetIdentity().GetKeyId())
	require.Equal(t, []string{"transactions:write"}, fc.GetScopes())
}
