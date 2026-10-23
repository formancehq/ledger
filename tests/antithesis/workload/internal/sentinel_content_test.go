package internal

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

const sentinelContentProperty = "committed sentinel transaction content survives operational events"

func sentinelTransactionFixture() *commonpb.Transaction {
	return &commonpb.Transaction{
		Id: 42, Reference: "committed-reference",
		Postings:          []*commonpb.Posting{commonpb.NewPosting("world", "sentinel:42", "COIN", big.NewInt(17))},
		Metadata:          map[string]*commonpb.MetadataValue{"witness": commonpb.NewStringValue("original")},
		Timestamp:         &commonpb.Timestamp{Data: 123},
		InsertedAt:        &commonpb.Timestamp{Data: 124},
		UpdatedAt:         &commonpb.Timestamp{Data: 124},
		PostCommitVolumes: &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{"sentinel:42": {}}},
	}
}

// This server keeps the acknowledged bytes separate from the later read. It
// exercises the real PreCommitSentinel/Verify RPC boundary without a cluster.
type sentinelContentServer struct {
	servicepb.UnimplementedBucketServiceServer

	acknowledged *commonpb.Transaction
	observed     *commonpb.Transaction
	applies      atomic.Int32
	reads        atomic.Int32
}

func (s *sentinelContentServer) Apply(context.Context, *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.applies.Add(1)

	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "sdk-sentinel",
			Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: s.acknowledged},
			}}},
		}}},
	}}}, nil
}

func (s *sentinelContentServer) GetTransaction(context.Context, *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	s.reads.Add(1)

	return &servicepb.GetTransactionResponse{Transaction: s.observed}, nil
}

func TestSentinelContentSDK(t *testing.T) {
	t.Parallel()
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	cases := []struct {
		name   string
		mutate func(*commonpb.Transaction) *commonpb.Transaction
	}{
		{"unchanged", func(tx *commonpb.Transaction) *commonpb.Transaction { return tx }},
		{"missing_payload", func(*commonpb.Transaction) *commonpb.Transaction { return nil }},
		{"identity", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.Id++

			return tx
		}},
		{"reference", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.Reference = "other"

			return tx
		}},
		{"posting", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.Postings[0] = commonpb.NewPosting("world", "sentinel:42", "COIN", big.NewInt(18))

			return tx
		}},
		{"metadata", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.Metadata["witness"] = commonpb.NewStringValue("changed")

			return tx
		}},
		{"timestamp", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.Timestamp = &commonpb.Timestamp{Data: 125}

			return tx
		}},
		{"inserted_at", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.InsertedAt = nil

			return tx
		}},
		{"updated_at", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.UpdatedAt = nil

			return tx
		}},
		{"reverted", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.Reverted = true

			return tx
		}},
		{"reverted_at", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.RevertedAt = &commonpb.Timestamp{Data: 125}

			return tx
		}},
		{"reverted_by", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.RevertedByTransaction = 43

			return tx
		}},
		{"reverts", func(tx *commonpb.Transaction) *commonpb.Transaction {
			tx.RevertsTransaction = 41

			return tx
		}},
		{"historical_volumes", func(tx *commonpb.Transaction) *commonpb.Transaction {
			delete(tx.GetPostCommitVolumes().GetVolumesByAccount(), "sentinel:42")

			return tx
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			events := sdktest.Capture(t, func() {
				acknowledged := sentinelTransactionFixture()
				server := &sentinelContentServer{acknowledged: acknowledged, observed: tc.mutate(proto.Clone(acknowledged).(*commonpb.Transaction))}
				client := sdktest.Client(t, server)
				sentinel, err := PreCommitSentinel(t.Context(), client, "sdk-sentinel")
				require.NoError(t, err)
				require.Equal(t, acknowledged.GetId(), sentinel.TxID)
				require.Equal(t, acknowledged.GetReference(), sentinel.Reference)
				sentinel.Verify(t.Context(), client, "after-operation")
				require.EqualValues(t, 1, server.applies.Load())
				require.EqualValues(t, 1, server.reads.Load())
			})
			if events == nil {
				return
			}
			event := sdktest.Find(t, events, sentinelContentProperty)
			require.Equal(t, "Always", event.DisplayType)
			require.True(t, event.MustHit)
			require.Equal(t, tc.name == "unchanged", event.Condition)
			if tc.name == "unchanged" {
				sdktest.Find(t, events, "sentinel transaction read-after-write succeeded")
			} else {
				sdktest.Absent(t, events, "sentinel transaction read-after-write succeeded")
			}
			sdktest.Absent(t, events, "committed sentinel transaction must survive operational events")
		})
	}
}

func TestSentinelOwnsAcknowledgedSnapshot(t *testing.T) {
	t.Parallel()
	transaction := sentinelTransactionFixture()
	before := proto.Clone(transaction).(*commonpb.Transaction)
	sentinel := newSentinel("sdk-sentinel", transaction)
	transaction.Reference = "mutated"
	transaction.Postings[0].Destination = "other"
	transaction.Metadata["witness"] = commonpb.NewStringValue("changed")
	transaction.Timestamp = nil
	delete(transaction.GetPostCommitVolumes().GetVolumesByAccount(), "sentinel:42")
	require.True(t, proto.Equal(before, sentinel.expected), "witness must own nested maps and messages, not alias the acknowledged response")
	require.Equal(t, before.GetReference(), sentinel.Reference)
}
