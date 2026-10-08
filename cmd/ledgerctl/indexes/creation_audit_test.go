package indexes

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func creationAuditFixture(t *testing.T) (*ledgerpb.AuditEntry, *ledgerpb.Index) {
	t.Helper()
	id := indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "main", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{Id: id}}}}}}}
	data, err := order.MarshalVT()
	require.NoError(t, err)
	stamp := &ledgerpb.Timestamp{Data: 1234}

	return &ledgerpb.AuditEntry{Sequence: 2, Timestamp: stamp, Idempotency: &ledgerpb.Idempotency{Key: "operator/uid/attempt"}, OrderCount: 1, Outcome: &ledgerpb.AuditEntry_Success{Success: &ledgerpb.AuditSuccess{MinLogSequence: 9, MaxLogSequence: 9}}, Items: []*ledgerpb.AuditItem{{SerializedOrder: data, LogSequence: 9}}}, &ledgerpb.Index{Id: id, CreatedAt: stamp}
}

func TestAttributedIndex(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*ledgerpb.AuditEntry){
		"other UID":       func(e *ledgerpb.AuditEntry) { e.Idempotency.Key = "operator/other/attempt" },
		"failed":          func(e *ledgerpb.AuditEntry) { e.Outcome = &ledgerpb.AuditEntry_Failure{Failure: &ledgerpb.AuditFailure{}} },
		"replay":          func(e *ledgerpb.AuditEntry) { e.Items[0].LogSequence = 0 },
		"not fresh":       func(e *ledgerpb.AuditEntry) { e.GetSuccess().MinLogSequence = 10 },
		"range mismatch":  func(e *ledgerpb.AuditEntry) { e.GetSuccess().MaxLogSequence = 10 },
		"multiple orders": func(e *ledgerpb.AuditEntry) { e.OrderCount = 2 },
		"multiple items":  func(e *ledgerpb.AuditEntry) { e.Items = append(e.Items, e.GetItems()[0]) },
		"missing item":    func(e *ledgerpb.AuditEntry) { e.Items = nil },
		"wrong position":  func(e *ledgerpb.AuditEntry) { e.Items[0].OrderIndex = 1 },
		"replacement":     func(e *ledgerpb.AuditEntry) { e.Timestamp = &ledgerpb.Timestamp{Data: 1235} },
		"missing date":    func(e *ledgerpb.AuditEntry) { e.Timestamp = nil },
		"other ledger": func(e *ledgerpb.AuditEntry) {
			o := &raftcmdpb.Order{}
			require.NoError(t, o.UnmarshalVT(e.GetItems()[0].GetSerializedOrder()))
			o.GetLedgerScoped().Ledger = "other"
			var err error
			e.Items[0].SerializedOrder, err = o.MarshalVT()
			require.NoError(t, err)
		},
		"other index": func(e *ledgerpb.AuditEntry) {
			o := &raftcmdpb.Order{}
			require.NoError(t, o.UnmarshalVT(e.GetItems()[0].GetSerializedOrder()))
			o.GetLedgerScoped().GetApply().GetCreateIndex().Id = indexes.TxBuiltinID(ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
			var err error
			e.Items[0].SerializedOrder, err = o.MarshalVT()
			require.NoError(t, err)
		},
		"wrong order": func(e *ledgerpb.AuditEntry) { e.Items[0].SerializedOrder = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, idx := creationAuditFixture(t)
			mutate(e)
			got, err := attributedIndex(e, "main", "operator/uid/", map[string]*ledgerpb.Index{indexes.Canonical(idx.GetId()): idx})
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
	e, idx := creationAuditFixture(t)
	got, err := attributedIndex(e, "main", "operator/uid/", map[string]*ledgerpb.Index{indexes.Canonical(idx.GetId()): idx})
	require.NoError(t, err)
	require.Equal(t, indexes.Canonical(idx.GetId()), got)
	e.Items[0].SerializedOrder = []byte{0xff}
	_, err = attributedIndex(e, "main", "operator/uid/", nil)
	require.ErrorContains(t, err, "decoding creation audit 2")
}

type creationAuditServer struct {
	ledgerpb.UnimplementedBucketServiceServer

	entry *ledgerpb.AuditEntry
	mode  string
}

// ListAuditEntries expects the idempotency-key prefix filter that
// filterIndexesByCreationKey now passes to avoid a full audit scan.
func (s *creationAuditServer) ListAuditEntries(req *ledgerpb.ListAuditEntriesRequest, stream grpc.ServerStreamingServer[ledgerpb.AuditEntry]) error {
	filter := req.GetOptions().GetFilter().GetAudit()
	if filter == nil || filter.GetField() != ledgerpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY || filter.GetStringPrefix() == "" {
		return status.Error(codes.InvalidArgument, "expected idempotency-key prefix filter")
	}
	if req.GetOptions().GetCursor() == "" {
		stream.SetTrailer(metadata.Pairs(cmdutil.NextCursorTrailerKey, "page2"))

		return stream.Send(&ledgerpb.AuditEntry{Sequence: 1})
	}
	if s.mode == "stream error" {
		return status.Error(codes.Unavailable, "audit unavailable")
	}
	header := proto.Clone(s.entry).(*ledgerpb.AuditEntry)
	header.Items = nil
	if s.mode == "repeated cursor" {
		stream.SetTrailer(metadata.Pairs(cmdutil.NextCursorTrailerKey, "page2"))
	}

	return stream.Send(header)
}
func (s *creationAuditServer) GetAuditEntry(_ context.Context, req *ledgerpb.GetAuditEntryRequest) (*ledgerpb.AuditEntry, error) {
	if s.mode == "get error" {
		return nil, status.Error(codes.Unavailable, "entry unavailable")
	}
	if req.GetSequence() != s.entry.GetSequence() {
		return nil, status.Error(codes.NotFound, "wrong sequence")
	}

	return s.entry, nil
}
func TestCreationAuditPagination(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"success", "stream error", "get error", "repeated cursor"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			entry, idx := creationAuditFixture(t)
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			ledgerpb.RegisterBucketServiceServer(server, &creationAuditServer{entry: entry, mode: mode})
			go func() { _ = server.Serve(listener) }() // Stop closes the listener.
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result, err := filterIndexesByCreationKey(ctx, ledgerpb.NewBucketServiceClient(conn), "main", "operator/uid/", []*ledgerpb.Index{idx})
			if mode != "success" {
				require.Error(t, err)
				require.Nil(t, result)

				return
			}
			require.NoError(t, err)
			require.Equal(t, []*ledgerpb.Index{idx}, result)
		})
	}
}
