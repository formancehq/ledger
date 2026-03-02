package analysis

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeCompactTransaction(ts uint64, postings []CompactPosting) CompactTransaction {
	return CompactTransaction{
		Postings:     postings,
		Timestamp:    ts,
		HasTimestamp: true,
	}
}

func makeCompactPosting(src, dst, asset string, amount uint64) CompactPosting {
	return CompactPosting{
		Source:      src,
		Destination: dst,
		Asset:       asset,
		Amount:      new(big.Int).SetUint64(amount),
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

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 100),
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

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("bank:main", "bank:fees", "USD", 10),
			makeCompactPosting("bank:main", "users:alice", "USD", 90),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION, resp.FlowPatterns[0].Structure)
}

func TestAnalyzeTransactions_MultiSource(t *testing.T) {
	t.Parallel()

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("users:alice", "bank:main", "USD", 50),
			makeCompactPosting("users:bob", "bank:main", "USD", 50),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_SOURCE, resp.FlowPatterns[0].Structure)
}

func TestAnalyzeTransactions_NormalizationUUID(t *testing.T) {
	t.Parallel()

	// Create 12 transactions with different UUID user addresses (>10 = default threshold)
	var txns []CompactTransaction
	for i := 0; i < 12; i++ {
		uuid := fmt.Sprintf("a0b1c2d3-e4f5-6789-abcd-0000000000%02x", i)
		txns = append(txns, makeCompactTransaction(uint64(1000000+i*1000000),
			[]CompactPosting{
				makeCompactPosting(fmt.Sprintf("users:%s:main", uuid), "bank:fees", "USD", 10),
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

	var txns []CompactTransaction
	for i := 0; i < 12; i++ {
		txns = append(txns, makeCompactTransaction(uint64(1000000+i*1000000),
			[]CompactPosting{
				makeCompactPosting(fmt.Sprintf("orders:%d", 1000+i), "bank:revenue", "EUR", 50),
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
	txns := []CompactTransaction{
		makeCompactTransaction(dayMicro*0, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 100),
		}),
		makeCompactTransaction(dayMicro*2, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 200),
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

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 100),
		}),
		makeCompactTransaction(2000000, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 300),
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

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 100),
		}),
		{
			Postings: []CompactPosting{
				makeCompactPosting("world", "bank:main", "USD", 50),
			},
			Timestamp:    2000000,
			HasTimestamp: true,
			Reverted:     true,
		},
	}

	resp := AnalyzeTransactions(txns, 0)
	assert.Equal(t, uint64(2), resp.TotalTransactions)
	assert.Equal(t, uint64(1), resp.TotalReverted)
}

func TestAnalyzeTransactions_MetadataKeys(t *testing.T) {
	t.Parallel()

	txns := []CompactTransaction{
		{
			Postings: []CompactPosting{
				makeCompactPosting("world", "bank:main", "USD", 100),
			},
			Timestamp:    1000000,
			HasTimestamp: true,
			MetadataKeys: []string{"category", "region"},
		},
		{
			Postings: []CompactPosting{
				makeCompactPosting("world", "bank:main", "USD", 200),
			},
			Timestamp:    2000000,
			HasTimestamp: true,
			MetadataKeys: []string{"category", "source"},
		},
	}

	resp := AnalyzeTransactions(txns, 0)
	require.Len(t, resp.FlowPatterns, 1)
	assert.Equal(t, []string{"category", "region", "source"}, resp.FlowPatterns[0].MetadataKeys)
}

func TestAnalyzeTransactions_GroupedBySignature(t *testing.T) {
	t.Parallel()

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 100),
		}),
		makeCompactTransaction(2000000, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 200),
		}),
		makeCompactTransaction(3000000, []CompactPosting{
			makeCompactPosting("bank:main", "bank:fees", "EUR", 10),
		}),
	}

	resp := AnalyzeTransactions(txns, 0)
	assert.Equal(t, uint64(3), resp.TotalTransactions)
	require.Len(t, resp.FlowPatterns, 2)

	// First pattern should have 2 transactions (highest count)
	assert.Equal(t, uint64(2), resp.FlowPatterns[0].TransactionCount)
	assert.Equal(t, uint64(1), resp.FlowPatterns[1].TransactionCount)
}
