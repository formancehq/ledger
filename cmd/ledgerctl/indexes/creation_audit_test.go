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

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func creationAuditFixture(t *testing.T) (*auditpb.AuditEntry, *commonpb.Index) {
	t.Helper()
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "main", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{Id: id}}}}}}}
	data, err := order.MarshalVT()
	require.NoError(t, err)
	stamp := &commonpb.Timestamp{Data: 1234}

	return &auditpb.AuditEntry{Sequence: 2, Timestamp: stamp, Idempotency: &commonpb.Idempotency{Key: "operator/uid/attempt"}, OrderCount: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 9, MaxLogSequence: 9}}, Items: []*auditpb.AuditItem{{SerializedOrder: data, LogSequence: 9}}}, &commonpb.Index{Id: id, CreatedAt: stamp}
}

func TestAttributedIndex(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*auditpb.AuditEntry){
		"other UID":       func(e *auditpb.AuditEntry) { e.Idempotency.Key = "operator/other/attempt" },
		"failed":          func(e *auditpb.AuditEntry) { e.Outcome = &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}} },
		"replay":          func(e *auditpb.AuditEntry) { e.Items[0].LogSequence = 0 },
		"not fresh":       func(e *auditpb.AuditEntry) { e.GetSuccess().MinLogSequence = 10 },
		"range mismatch":  func(e *auditpb.AuditEntry) { e.GetSuccess().MaxLogSequence = 10 },
		"multiple orders": func(e *auditpb.AuditEntry) { e.OrderCount = 2 },
		"multiple items":  func(e *auditpb.AuditEntry) { e.Items = append(e.Items, e.GetItems()[0]) },
		"missing item":    func(e *auditpb.AuditEntry) { e.Items = nil },
		"wrong position":  func(e *auditpb.AuditEntry) { e.Items[0].OrderIndex = 1 },
		"replacement":     func(e *auditpb.AuditEntry) { e.Timestamp = &commonpb.Timestamp{Data: 1235} },
		"missing date":    func(e *auditpb.AuditEntry) { e.Timestamp = nil },
		"other ledger": func(e *auditpb.AuditEntry) {
			o := &raftcmdpb.Order{}
			require.NoError(t, o.UnmarshalVT(e.GetItems()[0].GetSerializedOrder()))
			o.GetLedgerScoped().Ledger = "other"
			var err error
			e.Items[0].SerializedOrder, err = o.MarshalVT()
			require.NoError(t, err)
		},
		"other index": func(e *auditpb.AuditEntry) {
			o := &raftcmdpb.Order{}
			require.NoError(t, o.UnmarshalVT(e.GetItems()[0].GetSerializedOrder()))
			o.GetLedgerScoped().GetApply().GetCreateIndex().Id = indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
			var err error
			e.Items[0].SerializedOrder, err = o.MarshalVT()
			require.NoError(t, err)
		},
		"wrong order": func(e *auditpb.AuditEntry) { e.Items[0].SerializedOrder = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, idx := creationAuditFixture(t)
			mutate(e)
			got, err := attributedIndexes(e, "main", "operator/uid/", map[string]*commonpb.Index{indexes.Canonical(idx.GetId()): idx})
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
	e, idx := creationAuditFixture(t)
	got, err := attributedIndexes(e, "main", "operator/uid/", map[string]*commonpb.Index{indexes.Canonical(idx.GetId()): idx})
	require.NoError(t, err)
	require.Equal(t, []string{indexes.Canonical(idx.GetId())}, got)
	e.Items[0].SerializedOrder = []byte{0xff}
	_, err = attributedIndexes(e, "main", "operator/uid/", nil)
	require.ErrorContains(t, err, "decoding creation audit 2")
}

// creationBatchFixture is a ledger creation that carries its initial indexes
// (EN-2070): CreateLedger, then CreateIndex reference and timestamp, logged
// at 9, 10 and 11.
func creationBatchFixture(t *testing.T) (*auditpb.AuditEntry, map[string]*commonpb.Index) {
	t.Helper()
	marshal := func(order *raftcmdpb.Order) []byte {
		data, err := order.MarshalVT()
		require.NoError(t, err)

		return data
	}
	createIndex := func(id *commonpb.IndexID) []byte {
		return marshal(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "main",
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{Id: id}}}}}}})
	}
	reference := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	timestamp := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	stamp := &commonpb.Timestamp{Data: 1234}
	entry := &auditpb.AuditEntry{
		Sequence: 2, Timestamp: stamp, Idempotency: &commonpb.Idempotency{Key: "operator-ledger-create:uid:1"}, OrderCount: 3,
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 9, MaxLogSequence: 11}},
		Items: []*auditpb.AuditItem{
			{OrderIndex: 0, LogSequence: 9, SerializedOrder: marshal(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "main"}}})},
			{OrderIndex: 1, LogSequence: 10, SerializedOrder: createIndex(reference)},
			{OrderIndex: 2, LogSequence: 11, SerializedOrder: createIndex(timestamp)},
		},
	}

	return entry, map[string]*commonpb.Index{
		indexes.Canonical(reference): {Id: reference, CreatedAt: stamp},
		indexes.Canonical(timestamp): {Id: timestamp, CreatedAt: stamp},
	}
}

// Indexes created atomically with their ledger are attributed like
// standalone creations, so the operator can later drop the ones it declared.
func TestAttributedIndexesOfACreationBatch(t *testing.T) {
	t.Parallel()
	reference := indexes.Canonical(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
	timestamp := indexes.Canonical(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP))

	e, current := creationBatchFixture(t)
	got, err := attributedIndexes(e, "main", "operator-ledger-create:uid:", current)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{reference, timestamp}, got)

	// An index dropped and recreated since carries another creation date.
	e, current = creationBatchFixture(t)
	current[timestamp].CreatedAt = &commonpb.Timestamp{Data: 1235}
	got, err = attributedIndexes(e, "main", "operator-ledger-create:uid:", current)
	require.NoError(t, err)
	require.Equal(t, []string{reference}, got)

	// A batch that also drops or recreates an index cannot date it.
	for name, extra := range map[string]*raftcmdpb.LedgerApplyOrder{
		"dropped in the batch":   {Data: &raftcmdpb.LedgerApplyOrder_DropIndex{DropIndex: &raftcmdpb.DropIndexOrder{Id: indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)}}},
		"recreated in the batch": {Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{Id: indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, current := creationBatchFixture(t)
			data, err := (&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "main",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: extra}}}}).MarshalVT()
			require.NoError(t, err)
			e.Items = append(e.Items, &auditpb.AuditItem{OrderIndex: 3, LogSequence: 12, SerializedOrder: data})
			e.OrderCount, e.GetSuccess().MaxLogSequence = 4, 12
			got, err := attributedIndexes(e, "main", "operator-ledger-create:uid:", current)
			require.NoError(t, err)
			require.Equal(t, []string{reference}, got)
		})
	}

	malformed := map[string]func(*auditpb.AuditEntry){
		"replayed order":     func(e *auditpb.AuditEntry) { e.Items[1].LogSequence = 0 },
		"duplicate position": func(e *auditpb.AuditEntry) { e.Items[2].OrderIndex = 1 },
		"position past end":  func(e *auditpb.AuditEntry) { e.Items[2].OrderIndex = 3 },
		"duplicate sequence": func(e *auditpb.AuditEntry) { e.Items[2].LogSequence = 10 },
		"gap in range":       func(e *auditpb.AuditEntry) { e.Items[2].LogSequence = 12; e.GetSuccess().MaxLogSequence = 12 },
		"range mismatch":     func(e *auditpb.AuditEntry) { e.GetSuccess().MaxLogSequence = 12 },
		"missing item":       func(e *auditpb.AuditEntry) { e.Items = e.GetItems()[:2] },
		"other key":          func(e *auditpb.AuditEntry) { e.Idempotency.Key = "operator-ledger-create:other:1" },
	}
	for name, mutate := range malformed {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, current := creationBatchFixture(t)
			mutate(e)
			got, err := attributedIndexes(e, "main", "operator-ledger-create:uid:", current)
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
}

type creationAuditServer struct {
	servicepb.UnimplementedBucketServiceServer

	entry *auditpb.AuditEntry
	mode  string
}

// ListAuditEntries expects the idempotency-key prefix filter that
// filterIndexesByCreationKey now passes to avoid a full audit scan.
func (s *creationAuditServer) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream grpc.ServerStreamingServer[auditpb.AuditEntry]) error {
	filter := req.GetOptions().GetFilter().GetAudit()
	if filter == nil || filter.GetField() != commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY || filter.GetStringPrefix() == "" {
		return status.Error(codes.InvalidArgument, "expected idempotency-key prefix filter")
	}
	if req.GetOptions().GetCursor() == "" {
		stream.SetTrailer(metadata.Pairs(cmdutil.NextCursorTrailerKey, "page2"))

		return stream.Send(&auditpb.AuditEntry{Sequence: 1})
	}
	if s.mode == "stream error" {
		return status.Error(codes.Unavailable, "audit unavailable")
	}
	header := proto.Clone(s.entry).(*auditpb.AuditEntry)
	header.Items = nil
	if s.mode == "repeated cursor" {
		stream.SetTrailer(metadata.Pairs(cmdutil.NextCursorTrailerKey, "page2"))
	}

	return stream.Send(header)
}
func (s *creationAuditServer) GetAuditEntry(_ context.Context, req *servicepb.GetAuditEntryRequest) (*auditpb.AuditEntry, error) {
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
			servicepb.RegisterBucketServiceServer(server, &creationAuditServer{entry: entry, mode: mode})
			go func() { _ = server.Serve(listener) }() // Stop closes the listener.
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result, err := filterIndexesByCreationKey(ctx, servicepb.NewBucketServiceClient(conn), "main", "operator/uid/", []*commonpb.Index{idx})
			if mode != "success" {
				require.Error(t, err)
				require.Nil(t, result)

				return
			}
			require.NoError(t, err)
			require.Equal(t, []*commonpb.Index{idx}, result)
		})
	}
}
