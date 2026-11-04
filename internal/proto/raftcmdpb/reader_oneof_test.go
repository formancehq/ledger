package raftcmdpb_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestOneofReadersDoNotExposeMutableGroups(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		reader reflect.Type
		getter string
	}{
		{"order", reflect.TypeFor[raftcmdpb.OrderReader](), "GetType"},
		{"proposal", reflect.TypeFor[raftcmdpb.ProposalReader](), "GetPayload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, exposed := tc.reader.MethodByName(tc.getter)
			require.False(t, exposed, "reader must not expose protoc's mutable oneof wrapper")
		})
	}
}

// newReaderProposal reaches nested messages, lists, maps and another oneof
// through both the Proposal and pre-existing Order envelopes.
func newReaderProposal() *raftcmdpb.Proposal {
	tx := &raftcmdpb.CreateTransactionOrder{
		Postings: []*commonpb.Posting{{Source: "world", Destination: "users:1", Asset: "USD"}},
		Metadata: map[string]*commonpb.MetadataValue{
			"key": {Type: &commonpb.MetadataValue_NullValue{NullValue: &commonpb.NullValue{Original: "original"}}},
		},
	}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "ledger",
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: tx},
		}},
	}}}

	return &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
		Orders: []*raftcmdpb.Order{order},
	}}}
}

func TestProposalReader_OneofTraversalAndCloneIsolation(t *testing.T) {
	t.Parallel()
	proposal := newReaderProposal()
	before, err := proposal.MarshalVT()
	require.NoError(t, err)

	r := proposal.AsReader()
	require.Equal(t, raftcmdpb.Proposal_PayloadReaderCase_Batch, r.WhichPayload())
	require.Nil(t, r.GetSetClusterPolicy())
	order := r.GetBatch().GetOrders().Get(0)
	require.Equal(t, raftcmdpb.Order_TypeReaderCase_LedgerScoped, order.WhichType())
	require.Nil(t, order.GetSystemScoped())
	tx := order.GetLedgerScoped().GetApply().GetCreateTransaction()
	require.Equal(t, "world", tx.GetPostings().Get(0).GetSource())
	value, found := tx.GetMetadata().Get("key")
	require.True(t, found)
	require.Equal(t, "original", value.GetNullValue().GetOriginal())

	// Clone from several depths: every mutation must be detached, including
	// collections and nested oneof messages reachable through the variant.
	clone := r.Mutate()
	cloneTx := clone.GetBatch().GetOrders()[0].GetLedgerScoped().GetApply().GetCreateTransaction()
	cloneTx.Postings[0].Source = "changed"
	cloneTx.Metadata["key"].GetNullValue().Original = "changed"
	clone.GetBatch().Orders = append(clone.GetBatch().Orders, &raftcmdpb.Order{})
	nestedClone := tx.Mutate()
	nestedClone.Postings[0].Destination = "changed"
	value.Mutate().GetNullValue().Original = "changed"
	order.Mutate().GetLedgerScoped().Ledger = "changed"

	after, err := proposal.MarshalVT()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestProposalReader_PolicyCloneIsolation(t *testing.T) {
	t.Parallel()
	proposal := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetClusterPolicy{
		SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: &commonpb.ClusterPolicy{Revision: 7}},
	}}
	r := proposal.AsReader()
	require.Equal(t, raftcmdpb.Proposal_PayloadReaderCase_SetClusterPolicy, r.WhichPayload())
	require.Nil(t, r.GetBatch())
	require.Equal(t, uint64(7), r.GetSetClusterPolicy().GetPolicy().GetRevision())
	r.Mutate().GetSetClusterPolicy().Policy.Revision = 8
	r.GetSetClusterPolicy().Mutate().Policy.Revision = 9
	r.GetSetClusterPolicy().GetPolicy().Mutate().Revision = 10
	require.Equal(t, uint64(7), proposal.GetSetClusterPolicy().GetPolicy().GetRevision())
}

func TestProposalReader_OneofPresence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		proposal *raftcmdpb.Proposal
		want     raftcmdpb.Proposal_PayloadReaderCase
	}{
		{"unset", &raftcmdpb.Proposal{}, raftcmdpb.Proposal_PayloadReaderCase_Unset},
		{"nil batch", &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{}}, raftcmdpb.Proposal_PayloadReaderCase_Batch},
		{"typed nil batch", &raftcmdpb.Proposal{Payload: (*raftcmdpb.Proposal_Batch)(nil)}, raftcmdpb.Proposal_PayloadReaderCase_Batch},
		{"nil policy", &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetClusterPolicy{}}, raftcmdpb.Proposal_PayloadReaderCase_SetClusterPolicy},
		{"typed nil policy", &raftcmdpb.Proposal{Payload: (*raftcmdpb.Proposal_SetClusterPolicy)(nil)}, raftcmdpb.Proposal_PayloadReaderCase_SetClusterPolicy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := tc.proposal.AsReader()
			require.Equal(t, tc.want, r.WhichPayload())
			require.Nil(t, r.GetBatch())
			require.Nil(t, r.GetSetClusterPolicy())
		})
	}
	require.Nil(t, (*raftcmdpb.Proposal)(nil).AsReader())
}

func TestOrderReader_NilVariantPresence(t *testing.T) {
	t.Parallel()
	for _, order := range []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{}},
		{Type: (*raftcmdpb.Order_LedgerScoped)(nil)},
	} {
		r := order.AsReader()
		require.Equal(t, raftcmdpb.Order_TypeReaderCase_LedgerScoped, r.WhichType())
		require.Nil(t, r.GetLedgerScoped())
		require.Nil(t, r.GetSystemScoped())
	}
	require.Equal(t, raftcmdpb.Order_TypeReaderCase_Unset, (&raftcmdpb.Order{}).AsReader().WhichType())
}

func TestProposalReader_OneofTraversalAllocations(t *testing.T) {
	// AllocsPerRun serializes execution; keep this test outside t.Parallel.
	proposal := newReaderProposal()
	var source string
	allocations := testing.AllocsPerRun(100, func() {
		r := proposal.AsReader()
		if r.WhichPayload() != raftcmdpb.Proposal_PayloadReaderCase_Batch {
			panic("unexpected proposal variant")
		}
		source = r.GetBatch().GetOrders().Get(0).GetLedgerScoped().GetApply().GetCreateTransaction().GetPostings().Get(0).GetSource()
	})
	require.Equal(t, "world", source)
	require.Zero(t, allocations)
}

func BenchmarkProposalReader_OneofTraversal(b *testing.B) {
	proposal := newReaderProposal()
	b.ReportAllocs()
	for b.Loop() {
		r := proposal.AsReader()
		if r.WhichPayload() != raftcmdpb.Proposal_PayloadReaderCase_Batch ||
			r.GetBatch().GetOrders().Get(0).GetLedgerScoped().GetApply().GetCreateTransaction().GetPostings().Get(0).GetSource() != "world" {
			b.Fatal("unexpected proposal content")
		}
	}
}

func TestTechnicalUpdateReader_OneofBytesAreCopied(t *testing.T) {
	t.Parallel()
	update := &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: []byte{1, 2}}}
	r := update.AsReader()
	require.Equal(t, raftcmdpb.TechnicalUpdate_KindReaderCase_AuditKey, r.WhichKind())
	bytes := r.GetAuditKey()
	bytes[0] = 9
	require.Equal(t, []byte{1, 2}, update.GetAuditKey())
	r.Mutate().GetAuditKey()[1] = 9
	require.Equal(t, []byte{1, 2}, update.GetAuditKey())
	for _, empty := range []*raftcmdpb.TechnicalUpdate{
		{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{}},
		{Kind: (*raftcmdpb.TechnicalUpdate_AuditKey)(nil)},
	} {
		require.Equal(t, raftcmdpb.TechnicalUpdate_KindReaderCase_AuditKey, empty.AsReader().WhichKind())
		require.Nil(t, empty.AsReader().GetAuditKey())
	}
	require.Equal(t, raftcmdpb.TechnicalUpdate_KindReaderCase_Unset, (&raftcmdpb.TechnicalUpdate{}).AsReader().WhichKind())
}

// Isolate the new discriminator and message-arm views from existing ListReader
// interface boxing costs in the full batch traversal benchmark.
func BenchmarkProposalReader_OneofPolicy(b *testing.B) {
	proposal := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetClusterPolicy{
		SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: &commonpb.ClusterPolicy{Revision: 7}},
	}}
	b.ReportAllocs()
	for b.Loop() {
		r := proposal.AsReader()
		if r.WhichPayload() != raftcmdpb.Proposal_PayloadReaderCase_SetClusterPolicy ||
			r.GetSetClusterPolicy().GetPolicy().GetRevision() != 7 {
			b.Fatal("unexpected policy content")
		}
	}
}
