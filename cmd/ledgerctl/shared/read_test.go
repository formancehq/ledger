package shared

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	ledgerplugin "github.com/formancehq/ledger/misc/fctl-plugin"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	ledgerjson "github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

const readLargeID = uint64(9007199254740993)

// readServer is an actual gRPC service fixture, not a BucketServiceClient mock.
// It observes the wire request, context and trailers used by the adapter.
type readServer struct {
	servicepb.UnimplementedBucketServiceServer

	mu              sync.Mutex
	requests        map[string]proto.Message
	metadata        map[string]metadata.MD
	calls           map[string]int
	partialAccounts bool
	accountEntered  chan struct{}
	accountCanceled chan struct{}
}

func newReadServer(t *testing.T) (*readServer, servicepb.BucketServiceClient) {
	t.Helper()
	fixture := &readServer{requests: make(map[string]proto.Message), metadata: make(map[string]metadata.MD), calls: make(map[string]int)}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, fixture)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		serveErr := <-done
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			require.NoError(t, serveErr)
		}
	})
	conn, err := grpc.NewClient("passthrough:///read-fixture", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return fixture, servicepb.NewBucketServiceClient(conn)
}

func (s *readServer) record(ctx context.Context, name string, request proto.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests[name] = proto.Clone(request)
	md, _ := metadata.FromIncomingContext(ctx)
	s.metadata[name] = md.Copy()
	s.calls[name]++
}

func (s *readServer) observed(name string) (proto.Message, metadata.MD, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.requests[name], s.metadata[name].Copy(), s.calls[name]
}

func readLedger() *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{Name: "main", Metadata: map[string]*commonpb.MetadataValue{"exact": commonpb.NewUintValue(readLargeID)},
		MetadataSchema: &commonpb.MetadataSchema{AccountFields: map[string]*commonpb.MetadataFieldSchema{"when": {Type: commonpb.MetadataType_METADATA_TYPE_DATETIME}}}}
}

func readTransaction() *commonpb.Transaction {
	return &commonpb.Transaction{Id: readLargeID, Reference: "native", Metadata: map[string]*commonpb.MetadataValue{"exact": commonpb.NewUintValue(readLargeID)},
		Postings:          []*commonpb.Posting{commonpb.NewColoredPosting("world", "users:alice", "USD/2", "GOLD", new(big.Int).Lsh(big.NewInt(1), 100))},
		PostCommitVolumes: &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{"users:alice": {Volumes: []*commonpb.VolumeEntry{{Asset: "USD/2", Color: "GOLD", Volumes: &commonpb.Volumes{Input: commonpb.MustBigUintFromDecimal("1267650600228229401496703205376"), Output: commonpb.MustBigUintFromDecimal("0")}}}}}}}
}

func (s *readServer) ListLedgers(req *servicepb.ListLedgersRequest, stream grpc.ServerStreamingServer[commonpb.LedgerInfo]) error {
	s.record(stream.Context(), "ledgers", req)
	stream.SetTrailer(metadata.Pairs("x-next-cursor", (pagecursor.Cursor{Key: "main"}).Encode(), "x-previous-cursor", (pagecursor.Cursor{Key: "main", Back: true}).Encode()))

	return stream.Send(readLedger())
}
func (s *readServer) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	s.record(ctx, "ledger", req)

	return readLedger(), nil
}
func (s *readServer) GetLedgerStats(ctx context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
	s.record(ctx, "stats", req)

	return &commonpb.LedgerStats{TransactionCount: readLargeID}, nil
}
func (s *readServer) Discovery(ctx context.Context, req *servicepb.DiscoveryRequest) (*servicepb.DiscoveryResponse, error) {
	s.record(ctx, "info", req)

	return &servicepb.DiscoveryResponse{ServerInfo: &servicepb.ServerInfo{Version: "3.0.0", Commit: "test", GoVersion: "go1.27", ProtocolVersion: "26"}}, nil
}
func (s *readServer) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	s.record(ctx, "account", req)
	if s.accountEntered != nil {
		close(s.accountEntered)
		<-ctx.Done()
		close(s.accountCanceled)

		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err := grpc.SetTrailer(ctx, metadata.Pairs("x-query-profile", "native-profile")); err != nil {
		return nil, err
	}

	return &commonpb.Account{Address: req.GetAddress(), Metadata: map[string]*commonpb.MetadataValue{"exact": commonpb.NewUintValue(readLargeID)}}, nil
}
func (s *readServer) ListAccounts(req *servicepb.ListAccountsRequest, stream grpc.ServerStreamingServer[commonpb.Account]) error {
	s.record(stream.Context(), "accounts", req)
	stream.SetTrailer(metadata.Pairs("x-next-cursor", (pagecursor.Cursor{Key: "users:alice"}).Encode(), "x-previous-cursor", (pagecursor.Cursor{Key: "users:alice", Back: true}).Encode(), "x-query-profile", "native-profile"))
	if err := stream.Send(&commonpb.Account{Address: "users:alice"}); err != nil {
		return err
	}
	if s.partialAccounts {
		return status.Error(codes.Unavailable, "fixture interrupted after first record")
	}

	return nil
}
func (s *readServer) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	s.record(ctx, "transaction", req)

	return &servicepb.GetTransactionResponse{Transaction: readTransaction()}, nil
}
func (s *readServer) ListTransactions(req *servicepb.ListTransactionsRequest, stream grpc.ServerStreamingServer[commonpb.Transaction]) error {
	s.record(stream.Context(), "transactions", req)
	stream.SetTrailer(metadata.Pairs("x-next-cursor", (pagecursor.Cursor{Key: "9007199254740993"}).Encode()))

	return stream.Send(readTransaction())
}
func (s *readServer) ListLogs(req *servicepb.ListLogsRequest, stream grpc.ServerStreamingServer[commonpb.Log]) error {
	s.record(stream.Context(), "logs", req)
	stream.SetTrailer(metadata.Pairs("x-next-cursor", (pagecursor.Cursor{Key: "7"}).Encode()))

	return stream.Send(&commonpb.Log{Sequence: readLargeID, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "main", Log: &commonpb.LedgerLog{Id: 7, Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: readTransaction()}}}}}}}})
}
func (s *readServer) AggregateVolumes(ctx context.Context, req *servicepb.AggregateVolumesRequest) (*commonpb.AggregateResult, error) {
	s.record(ctx, "aggregate", req)
	volume := &commonpb.AggregatedVolume{Asset: "USD/2", Color: "GOLD", Input: commonpb.NewUint256FromUint64(readLargeID), Output: commonpb.NewUint256FromUint64(readLargeID + 1)}

	return &commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{volume}, Groups: []*commonpb.GroupedAggregateResult{{Prefix: "users:", Volumes: []*commonpb.AggregatedVolume{volume}}}}, nil
}
func (s *readServer) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream grpc.ServerStreamingServer[auditpb.AuditEntry]) error {
	s.record(stream.Context(), "audit", req)

	return status.Error(codes.PermissionDenied, "creation attribution requires audit read")
}

func readIndex() *commonpb.Index {
	return &commonpb.Index{Id: indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "when")}
}
func (s *readServer) ListIndexes(req *servicepb.ListIndexesRequest, stream grpc.ServerStreamingServer[commonpb.Index]) error {
	s.record(stream.Context(), "indexes", req)

	return stream.Send(readIndex())
}
func (s *readServer) GetIndex(ctx context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
	s.record(ctx, "index", req)

	return readIndex(), nil
}
func (s *readServer) GetIndexEntryStatus(ctx context.Context, req *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
	s.record(ctx, "index-status", req)

	return &servicepb.IndexEntry{Ledger: req.GetLedger(), Index: readIndex(), Cursor: readLargeID, CurrentVersion: 2}, nil
}
func (s *readServer) InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
	s.record(ctx, "inspect", req)
	when := commonpb.NewIntValue(1700000000123456)
	switch req.GetMode() {
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES:
		return &servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_DistinctValues{DistinctValues: &servicepb.InspectDistinctValues{Values: []*commonpb.MetadataValue{when}, HasMore: true, NextCursor: "next-inspection", PreviousCursor: "previous-inspection"}}}, nil
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS:
		return &servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_Facets{Facets: &servicepb.InspectFacets{Facets: []*servicepb.InspectFacet{{Value: when, Count: readLargeID}}, HasMore: true, NextCursor: "next-inspection"}}}, nil
	default:
		return &servicepb.InspectIndexResponse{Result: &servicepb.InspectIndexResponse_Summary{Summary: &servicepb.InspectSummary{Cardinality: readLargeID, Min: when, Max: when, EntitiesWithKey: readLargeID}}}, nil
	}
}

func readRequest(path string, args ...string) pluginsdk.ExecuteRequest {
	return pluginsdk.ExecuteRequest{CommandPath: strings.Split(path, "/"), Args: args,
		Flags: map[string]string{"ledger": "main", "page-size": "2"}, Context: make(map[string]string)}
}

func TestReadCommandsUseNativeRPCAndHTTPEnvelope(t *testing.T) {
	t.Parallel()
	cases := []struct {
		path  string
		args  []string
		rpc   string
		paged bool
		want  string
	}{
		{"ledger/list", nil, "ledgers", true, `"name": "main"`},
		{"ledger/show", []string{"main"}, "ledger", false, `"name": "main"`},
		{"ledger/metadata/show", nil, "ledger", false, `"exact": 9007199254740993`},
		{"ledger/stats", nil, "stats", false, `"transactionCount": 9007199254740993`},
		{"ledger/info", nil, "info", false, `"protocolVersion": "26"`},
		{"ledger/balances", nil, "aggregate", false, `"balance": -1`},
		{"ledger/accounts/list", nil, "accounts", true, `"address": "users:alice"`},
		{"ledger/accounts/show", []string{"users:alice"}, "account", false, `"exact": 9007199254740993`},
		{"ledger/accounts/balances", []string{"users:alice"}, "account", false, `"volumes": []`},
		{"ledger/accounts/metadata/show", []string{"users:alice"}, "account", false, `"address": "users:alice"`},
		{"ledger/transactions/list", nil, "transactions", true, `1267650600228229401496703205376`},
		{"ledger/transactions/show", []string{"9007199254740993"}, "transaction", false, `"transaction": {`},
		{"ledger/transactions/metadata/show", []string{"9007199254740993"}, "transaction", false, `"id": 9007199254740993`},
		{"ledger/logs/list", nil, "logs", true, `"sequence": 9007199254740993`},
		{"ledger/indexes/list", nil, "indexes", false, `"metadata": {`},
		{"ledger/indexes/show", []string{"metadata:TARGET_TYPE_ACCOUNT:when"}, "index", false, `"key": "when"`},
		{"ledger/indexes/status", []string{"metadata:TARGET_TYPE_ACCOUNT:when"}, "index-status", false, `"currentVersion": 2`},
		{"ledger/indexes/inspect", []string{"metadata:TARGET_TYPE_ACCOUNT:when"}, "inspect", false, `"cardinality": 9007199254740993`},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			fixture, client := newReadServer(t)
			executor := &Executor{Client: client, CheckpointID: readLargeID, Consistency: "stale"}
			request := readRequest(tc.path, tc.args...)
			response, handled, err := executor.read(t.Context(), request)
			require.True(t, handled)
			require.NoError(t, err)
			require.True(t, json.Valid(response.Data), string(response.Data))
			require.Contains(t, string(response.Data), tc.want)
			require.NotNil(t, executor.Result)
			_, md, calls := fixture.observed(tc.rpc)
			require.Equal(t, 1, calls)
			require.Equal(t, []string{"stale"}, md.Get("x-consistency"))
			var envelope map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(response.Data, &envelope))
			if tc.path == "ledger/info" {
				require.NotContains(t, envelope, "data")
			} else {
				require.Contains(t, envelope, "data")
			}
			if tc.paged {
				require.NotEmpty(t, executor.NextCursor)
				require.Equal(t, json.RawMessage("true"), envelope["hasMore"])
				var next string
				require.NoError(t, json.Unmarshal(envelope["next"], &next))
				require.Equal(t, executor.NextCursor, next)
			}
		})
	}
}

func TestReadListPreservesPaginationFiltersDatesAndCheckpoint(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	executor := &Executor{Client: client, CheckpointID: 42}
	request := readRequest("ledger/transactions/list")
	request.Flags["filter"] = `{"$match":{"reference":"native"}}`
	request.Flags["reverse"] = "true"
	request.Flags["start-date"] = "2026-10-01T00:00:00Z"
	request.Flags["end-date"] = "2026-10-09T00:00:00Z"
	request.Context["cursor"] = (pagecursor.Cursor{Key: "9007199254740993", Back: true}).Encode()
	request.Context["checkpoint"] = "9007199254740993"
	_, handled, err := executor.read(t.Context(), request)
	require.True(t, handled)
	require.NoError(t, err)
	observed, _, _ := fixture.observed("transactions")
	options := observed.(*servicepb.ListTransactionsRequest).GetOptions()
	require.Equal(t, uint32(2), options.GetPageSize())
	require.Equal(t, readLargeID, options.GetRead().GetCheckpointId())
	require.Equal(t, request.Context["cursor"], options.GetCursor())
	require.True(t, options.GetReverse())
	require.Len(t, options.GetFilter().GetAnd().GetFilters(), 2)
	date := options.GetFilter().GetAnd().GetFilters()[0].GetBuiltinUint()
	require.Equal(t, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, date.GetField())
	require.Equal(t, uint64(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixMicro()), date.GetCond().GetMin())
	require.True(t, date.GetCond().GetMaxExclusive())
}

func TestReadPreservesPartialStreamAndTrailersWithoutRetry(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	fixture.partialAccounts = true
	executor := &Executor{Client: client}
	response, handled, err := executor.read(t.Context(), readRequest("ledger/accounts/list"))
	require.True(t, handled)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, string(response.Data), `"address": "users:alice"`)
	rows, ok := executor.Result.([]*commonpb.Account)
	require.True(t, ok)
	require.Len(t, rows, 1)
	require.NotEmpty(t, executor.NextCursor)
	require.NotEmpty(t, executor.PreviousCursor)
	require.Equal(t, []string{"native-profile"}, executor.Trailer.Get("x-query-profile"))
	_, _, calls := fixture.observed("accounts")
	require.Equal(t, 1, calls)
}

func TestReadCancellationReachesInflightRPC(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	fixture.accountEntered, fixture.accountCanceled = make(chan struct{}), make(chan struct{})
	executor := &Executor{Client: client}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := executor.read(ctx, readRequest("ledger/accounts/show", "users:alice"))
		done <- err
	}()
	select {
	case <-fixture.accountEntered:
	case <-ctx.Done():
		t.Fatal("RPC was never entered")
	}
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-done))
	select {
	case <-fixture.accountCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("server RPC did not observe cancellation")
	}
	_, _, calls := fixture.observed("account")
	require.Equal(t, 1, calls)
}

func TestReadInspectionUsesDeclaredDatetimeAndOwnCursor(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"summary", "distinctValues", "distinct-values", "facets"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fixture, client := newReadServer(t)
			executor := &Executor{Client: client, CheckpointID: 23}
			request := readRequest("ledger/indexes/inspect", "metadata:TARGET_TYPE_ACCOUNT:when")
			request.Flags["mode"] = mode
			request.Flags["cursor"] = "current-inspection"
			response, handled, err := executor.read(t.Context(), request)
			require.True(t, handled)
			require.NoError(t, err)
			require.Contains(t, string(response.Data), "2023-11-14T22:13:20.123456Z")
			observed, _, calls := fixture.observed("inspect")
			require.Equal(t, 1, calls)
			params := observed.(*servicepb.InspectIndexRequest)
			require.Equal(t, uint64(23), params.GetCheckpointId())
			require.Equal(t, "current-inspection", params.GetCursor())
			require.Equal(t, "when", params.GetMetadataKey())
			require.Equal(t, commonpb.MetadataType_METADATA_TYPE_DATETIME, executor.InspectMetadataType)
			if mode != "summary" {
				require.Equal(t, "next-inspection", executor.NextCursor)
				require.Contains(t, string(response.Data), `"nextCursor": "next-inspection"`)
			}
		})
	}
}

func TestReadRejectsInvalidOptionsBeforeRPC(t *testing.T) {
	t.Parallel()
	for name, configure := range map[string]func(*pluginsdk.ExecuteRequest){
		"checkpoint": func(r *pluginsdk.ExecuteRequest) { r.Context["checkpoint"] = "-1" },
		"cursor":     func(r *pluginsdk.ExecuteRequest) { r.Context["cursor"] = "not-a-cursor" },
		"date":       func(r *pluginsdk.ExecuteRequest) { r.Flags["start-date"] = "1969-12-31T23:59:59Z" },
		"filter":     func(r *pluginsdk.ExecuteRequest) { r.Flags["filter"] = "!malformed" },
		"size":       func(r *pluginsdk.ExecuteRequest) { r.Flags["page-size"] = "4294967296" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture, client := newReadServer(t)
			executor := &Executor{Client: client}
			request := readRequest("ledger/transactions/list")
			configure(&request)
			_, handled, err := executor.read(t.Context(), request)
			require.True(t, handled)
			require.Error(t, err)
			_, _, calls := fixture.observed("transactions")
			require.Zero(t, calls)
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	executor := &Executor{}
	_, handled, err := executor.read(ctx, readRequest("ledger/accounts/list"))
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
	_, handled, err = executor.read(t.Context(), readRequest("ledger/transactions/create"))
	require.False(t, handled)
	require.NoError(t, err)
}

func TestReadAccountAndAggregatePreserveNativeOptions(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	executor := &Executor{Client: client, CheckpointID: readLargeID, Consistency: "linearizable"}
	account := readRequest("ledger/accounts/show", "users:alice")
	account.Flags["collapse-colors"] = "true"
	account.Flags["consistency"] = " STALE "
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-consistency", "linearizable", "ledger-protocol-version", "26"))
	_, handled, err := executor.read(ctx, account)
	require.True(t, handled)
	require.NoError(t, err)
	observed, md, _ := fixture.observed("account")
	require.True(t, observed.(*servicepb.GetAccountRequest).GetCollapseColors())
	require.Equal(t, readLargeID, observed.(*servicepb.GetAccountRequest).GetCheckpointId())
	require.Equal(t, []string{"stale"}, md.Get("x-consistency"))
	require.Equal(t, []string{"26"}, md.Get("ledger-protocol-version"))
	require.Equal(t, []string{"native-profile"}, executor.Trailer.Get("x-query-profile"))

	aggregate := readRequest("ledger/balances")
	aggregate.Flags["use-max-precision"] = "true"
	aggregate.Flags["collapse-colors"] = "true"
	aggregate.Flags["group-by-prefixes"] = "users:,merchants:"
	aggregate.Context["prefix"] = "users:"
	response, handled, err := executor.read(t.Context(), aggregate)
	require.True(t, handled)
	require.NoError(t, err)
	observed, _, _ = fixture.observed("aggregate")
	params := observed.(*servicepb.AggregateVolumesRequest)
	require.True(t, params.GetUseMaxPrecision())
	require.True(t, params.GetCollapseColors())
	require.Equal(t, []string{"users:", "merchants:"}, params.GetGroupByPrefixes())
	require.Equal(t, readLargeID, params.GetCheckpointId())
	require.Equal(t, "users:", params.GetFilter().GetAddress().GetHardcodedPrefix())
	require.JSONEq(t, `{"data":{"volumes":[{"asset":"USD/2","color":"GOLD","input":9007199254740993,"output":9007199254740994,"balance":-1}],"groups":[{"prefix":"users:","volumes":[{"asset":"USD/2","color":"GOLD","input":9007199254740993,"output":9007199254740994,"balance":-1}]}]}}`, string(response.Data))
}

func TestReadAccountsAfterBecomesOpaqueCursor(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	executor := &Executor{Client: client}
	request := readRequest("ledger/accounts/list")
	request.Flags["after"] = "users:alice"
	_, handled, err := executor.read(t.Context(), request)
	require.True(t, handled)
	require.NoError(t, err)
	observed, _, _ := fixture.observed("accounts")
	token := observed.(*servicepb.ListAccountsRequest).GetOptions().GetCursor()
	decoded, err := pagecursor.Decode(token)
	require.NoError(t, err)
	require.Equal(t, "users:alice", decoded.Key)
	require.False(t, decoded.Back)

	request.Context["cursor"] = (pagecursor.Cursor{Key: "users:bob"}).Encode()
	_, handled, err = executor.read(t.Context(), request)
	require.True(t, handled)
	require.NoError(t, err)
	observed, _, calls := fixture.observed("accounts")
	require.Equal(t, 2, calls)
	require.Equal(t, request.Context["cursor"], observed.(*servicepb.ListAccountsRequest).GetOptions().GetCursor())
}

func TestReadIndexesCreationPrefixUsesAuditedAttribution(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	executor := &Executor{Client: client}
	request := readRequest("ledger/indexes/list")
	request.Context["creation-key-prefix"] = "deploy/operator/"
	response, handled, err := executor.read(t.Context(), request)
	require.True(t, handled)
	require.ErrorContains(t, err, "creation attribution requires audit read")
	require.Empty(t, response.Data, "unattributed entries must not be exposed as matching the prefix")
	require.Nil(t, executor.Result)
	observed, _, calls := fixture.observed("audit")
	require.Equal(t, 1, calls)
	filter := observed.(*servicepb.ListAuditEntriesRequest).GetOptions().GetFilter().GetAudit()
	require.Equal(t, commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, filter.GetField())
	require.Equal(t, request.Context["creation-key-prefix"], filter.GetStringPrefix())
}

func TestReadCancellationClearsStaleResultAndTrailers(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	executor := &Executor{Result: []*commonpb.Account{{Address: "previous"}}, NextCursor: "previous-next", PreviousCursor: "previous-back", Trailer: metadata.Pairs("x-query-profile", "previous")}
	response, handled, err := executor.read(ctx, readRequest("ledger/accounts/show", "users:alice"))
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, response.Data)
	require.Nil(t, executor.Result)
	require.Empty(t, executor.NextCursor)
	require.Empty(t, executor.PreviousCursor)
	require.Empty(t, executor.Trailer)
}

func TestReadRunsThroughSharedProductContract(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	executor := &Executor{Client: client, CheckpointID: readLargeID}
	plugin := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute)
	response, err := plugin.Execute(t.Context(), pluginsdk.ExecuteRequest{
		CommandPath: []string{"ledger", "transactions", "show"}, Args: []string{"9007199254740993"},
		Flags: map[string]string{"ledger": "main", "consistency": " STALE "},
	})
	require.NoError(t, err)
	require.Contains(t, string(response.Data), `"id": 9007199254740993`)
	require.Contains(t, string(response.Data), `1267650600228229401496703205376`)
	observed, md, calls := fixture.observed("transaction")
	require.Equal(t, 1, calls)
	require.Equal(t, readLargeID, observed.(*servicepb.GetTransactionRequest).GetTransactionId())
	require.Equal(t, readLargeID, observed.(*servicepb.GetTransactionRequest).GetCheckpointId())
	require.Equal(t, []string{"stale"}, md.Get("x-consistency"))

	_, err = plugin.Execute(t.Context(), pluginsdk.ExecuteRequest{
		CommandPath: []string{"ledger", "transactions", "show"}, Args: []string{"-1"}, Flags: map[string]string{"ledger": "main"},
	})
	require.ErrorContains(t, err, "unsigned 64-bit")
	_, _, calls = fixture.observed("transaction")
	require.Equal(t, 1, calls, "shared validation must run before the transport")
}

func TestReadSharedProductAfterAliasAndNextPage(t *testing.T) {
	t.Parallel()
	fixture, client := newReadServer(t)
	executor := &Executor{Client: client}
	plugin := ledgerplugin.NewWithExecutor("3.0.0", executor.Execute)
	request := pluginsdk.ExecuteRequest{
		CommandPath:  []string{"ledger", "accounts", "list"},
		Flags:        map[string]string{"ledger": "main", "after": "users:000", "page-size": "2"},
		ChangedFlags: map[string]bool{"after": true, "page-size": true},
	}
	_, err := plugin.Execute(t.Context(), request)
	require.NoError(t, err)
	observed, _, calls := fixture.observed("accounts")
	require.Equal(t, 1, calls)
	params := observed.(*servicepb.ListAccountsRequest).GetOptions()
	require.Equal(t, uint32(2), params.GetPageSize())
	cursor, err := pagecursor.Decode(params.GetCursor())
	require.NoError(t, err)
	require.Equal(t, "users:000", cursor.Key)

	next := executor.NextCursor
	request.Context = map[string]string{"cursor": next}
	_, err = plugin.Execute(t.Context(), request)
	require.NoError(t, err)
	observed, _, calls = fixture.observed("accounts")
	require.Equal(t, 2, calls)
	params = observed.(*servicepb.ListAccountsRequest).GetOptions()
	require.Equal(t, uint32(2), params.GetPageSize())
	require.Equal(t, next, params.GetCursor(), "resume must use the returned cursor rather than the initial after key")

	request.Flags["cursor"] = next
	_, err = plugin.Execute(t.Context(), request)
	require.ErrorContains(t, err, "--after and --cursor")
	_, _, calls = fixture.observed("accounts")
	require.Equal(t, 2, calls, "conflicting user flags must fail before transport dispatch")
}

func TestReadSDKMonetaryEncodingMatchesHTTPWithoutChangingNativeResult(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"ledger/transactions/show", "ledger/transactions/list", "ledger/logs/list"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			_, client := newReadServer(t)
			executor := &Executor{Client: client}
			request := readRequest(path)
			if path == "ledger/transactions/show" {
				request.Args = []string{"9007199254740993"}
			}
			response, _, err := executor.read(t.Context(), request)
			require.NoError(t, err)
			require.Contains(t, string(response.Data), `"input": 1267650600228229401496703205376`)
			require.Contains(t, string(response.Data), `"amount": 1267650600228229401496703205376`)
			native, err := cmdutil.MarshalJSON(executor.Result)
			require.NoError(t, err)
			require.Contains(t, string(native), `"input": "1267650600228229401496703205376"`)

			var envelope any
			switch path {
			case "ledger/transactions/show":
				envelope = map[string]any{"data": map[string]any{"transaction": executor.Result.(*servicepb.GetTransactionResponse).GetTransaction()}}
			default:
				envelope = map[string]any{"data": executor.Result, "next": executor.NextCursor, "hasMore": true}
			}
			httpJSON, err := ledgerjson.MarshalWithOptions(envelope, commonpb.MonetaryAmountsAsNumbers())
			require.NoError(t, err)
			require.JSONEq(t, string(httpJSON), string(response.Data))
			if path == "ledger/logs/list" {
				logs := executor.Result.([]*commonpb.Log)
				require.Equal(t, readLargeID, logs[0].GetSequence())
				require.Equal(t, uint64(7), logs[0].GetPayload().GetApply().GetLog().GetId())
				cursor, err := pagecursor.Decode(executor.NextCursor)
				require.NoError(t, err)
				require.Equal(t, "7", cursor.Key, "log pagination uses the local log ID rather than the system sequence")
			}
		})
	}
}
