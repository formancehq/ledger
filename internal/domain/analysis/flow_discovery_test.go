package analysis

import (
	"fmt"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTransaction(id uint64, ts uint64, postings []*commonpb.Posting) *commonpb.Transaction {
	return &commonpb.Transaction{
		Id:       id,
		Postings: postings,
		Timestamp: &commonpb.Timestamp{
			Data: ts,
		},
	}
}

func makePosting(src, dst, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      src,
		Destination: dst,
		Asset:       asset,
		Amount:      commonpb.NewUint256FromUint64(amount),
	}
}

func TestAnalyzeTransactions_Empty(t *testing.T) {
	t.Parallel()

	resp := AnalyzeTransactions(nil, 0)
	require.NotNil(t, resp)
	assert.Equal(t, uint64(0), resp.TotalTransactions)
	assert.Equal(t, uint64(0), resp.TotalReverted)
	assert.Empty(t, resp.FlowPatterns)
}

func TestAnalyzeTransactions_SingleSimple(t *testing.T) {
	t.Parallel()

	txns := []*commonpb.Transaction{
		makeTransaction(1, 1000000, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 100),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	require.NotNil(t, resp)
	assert.Equal(t, uint64(1), resp.TotalTransactions)
	assert.Equal(t, uint64(0), resp.TotalReverted)
	require.Len(t, resp.FlowPatterns, 1)

	pattern := resp.FlowPatterns[0]
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE, pattern.Structure)
	assert.Equal(t, uint64(1), pattern.TransactionCount)
	require.Len(t, pattern.Postings, 1)
	assert.Equal(t, "world", pattern.Postings[0].SourcePattern)
	assert.Equal(t, "bank:main", pattern.Postings[0].DestinationPattern)
	assert.Equal(t, "USD", pattern.Postings[0].Asset)
}

func TestAnalyzeTransactions_MultiDestination(t *testing.T) {
	t.Parallel()

	txns := []*commonpb.Transaction{
		makeTransaction(1, 1000000, []*commonpb.Posting{
			makePosting("bank:main", "bank:fees", "USD", 10),
			makePosting("bank:main", "users:alice", "USD", 90),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION, resp.FlowPatterns[0].Structure)
}

func TestAnalyzeTransactions_MultiSource(t *testing.T) {
	t.Parallel()

	txns := []*commonpb.Transaction{
		makeTransaction(1, 1000000, []*commonpb.Posting{
			makePosting("users:alice", "bank:main", "USD", 50),
			makePosting("users:bob", "bank:main", "USD", 50),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_SOURCE, resp.FlowPatterns[0].Structure)
}

func TestAnalyzeTransactions_NormalizationUUID(t *testing.T) {
	t.Parallel()

	// Create 12 transactions with different UUID user addresses (>10 = default threshold)
	var txns []*commonpb.Transaction
	for i := 0; i < 12; i++ {
		uuid := fmt.Sprintf("a0b1c2d3-e4f5-6789-abcd-0000000000%02x", i)
		txns = append(txns, makeTransaction(uint64(i+1), uint64(1000000+i*1000000),
			[]*commonpb.Posting{
				makePosting(fmt.Sprintf("users:%s:main", uuid), "bank:fees", "USD", 10),
			},
		))
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)

	pattern := resp.FlowPatterns[0]
	require.Len(t, pattern.Postings, 1)
	assert.Equal(t, "users:{id}:main", pattern.Postings[0].SourcePattern)
	assert.Equal(t, "bank:fees", pattern.Postings[0].DestinationPattern)
}

func TestAnalyzeTransactions_NormalizationNumeric(t *testing.T) {
	t.Parallel()

	var txns []*commonpb.Transaction
	for i := 0; i < 12; i++ {
		txns = append(txns, makeTransaction(uint64(i+1), uint64(1000000+i*1000000),
			[]*commonpb.Posting{
				makePosting(fmt.Sprintf("orders:%d", 1000+i), "bank:revenue", "EUR", 50),
			},
		))
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)

	pattern := resp.FlowPatterns[0]
	require.Len(t, pattern.Postings, 1)
	assert.Equal(t, "orders:{number}", pattern.Postings[0].SourcePattern)
}

func TestAnalyzeTransactions_TemporalStats(t *testing.T) {
	t.Parallel()

	// 1 microsecond = 1, so 1 day = 86400 * 1_000_000 microseconds
	dayMicro := uint64(86400 * 1_000_000)
	txns := []*commonpb.Transaction{
		makeTransaction(1, dayMicro*0, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 100),
		}),
		makeTransaction(2, dayMicro*2, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 200),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)

	temporal := resp.FlowPatterns[0].Temporal
	require.NotNil(t, temporal)
	assert.Equal(t, dayMicro*0, temporal.FirstSeen.Data)
	assert.Equal(t, dayMicro*2, temporal.LastSeen.Data)
	assert.InDelta(t, 1.0, temporal.TransactionsPerDay, 0.1)
}

func TestAnalyzeTransactions_VolumeStats(t *testing.T) {
	t.Parallel()

	txns := []*commonpb.Transaction{
		makeTransaction(1, 1000000, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 100),
		}),
		makeTransaction(2, 2000000, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 300),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)

	volumeStats := resp.FlowPatterns[0].VolumeStats
	require.Len(t, volumeStats, 1)
	assert.Equal(t, "USD", volumeStats[0].Asset)
	assert.Equal(t, "400", volumeStats[0].TotalVolume)
	assert.Equal(t, "200", volumeStats[0].AverageVolume)
	assert.Equal(t, "100", volumeStats[0].MinVolume)
	assert.Equal(t, "300", volumeStats[0].MaxVolume)
	assert.Equal(t, uint64(2), volumeStats[0].TransactionCount)
}

func TestAnalyzeTransactions_RevertedCounted(t *testing.T) {
	t.Parallel()

	txns := []*commonpb.Transaction{
		makeTransaction(1, 1000000, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 100),
		}),
		{
			Id:       2,
			Reverted: true,
			Postings: []*commonpb.Posting{
				makePosting("world", "bank:main", "USD", 50),
			},
			Timestamp: &commonpb.Timestamp{Data: 2000000},
		},
	}

	resp := AnalyzeTransactions(txns, 0)
	assert.Equal(t, uint64(2), resp.TotalTransactions)
	assert.Equal(t, uint64(1), resp.TotalReverted)
}

func TestAnalyzeTransactions_MetadataKeys(t *testing.T) {
	t.Parallel()

	txns := []*commonpb.Transaction{
		{
			Id: 1,
			Postings: []*commonpb.Posting{
				makePosting("world", "bank:main", "USD", 100),
			},
			Timestamp: &commonpb.Timestamp{Data: 1000000},
			Metadata: &commonpb.MetadataSet{
				Metadata: []*commonpb.Metadata{
					{Key: "category", Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "payment"}}},
					{Key: "region", Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "EU"}}},
				},
			},
		},
		{
			Id: 2,
			Postings: []*commonpb.Posting{
				makePosting("world", "bank:main", "USD", 200),
			},
			Timestamp: &commonpb.Timestamp{Data: 2000000},
			Metadata: &commonpb.MetadataSet{
				Metadata: []*commonpb.Metadata{
					{Key: "category", Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "refund"}}},
					{Key: "source", Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "api"}}},
				},
			},
		},
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)
	assert.Equal(t, []string{"category", "region", "source"}, resp.FlowPatterns[0].MetadataKeys)
}

func TestAnalyzeTransactions_GroupedBySignature(t *testing.T) {
	t.Parallel()

	txns := []*commonpb.Transaction{
		makeTransaction(1, 1000000, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 100),
		}),
		makeTransaction(2, 2000000, []*commonpb.Posting{
			makePosting("world", "bank:main", "USD", 200),
		}),
		makeTransaction(3, 3000000, []*commonpb.Posting{
			makePosting("bank:main", "bank:fees", "EUR", 10),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	assert.Equal(t, uint64(3), resp.TotalTransactions)
	require.Len(t, resp.FlowPatterns, 2)

	// First pattern should have 2 transactions (highest count)
	assert.Equal(t, uint64(2), resp.FlowPatterns[0].TransactionCount)
	assert.Equal(t, uint64(1), resp.FlowPatterns[1].TransactionCount)
}
