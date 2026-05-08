package analysis

import (
	"fmt"
	"io"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// analyzeTransactions is a test helper that wraps AnalyzeTransactionsFromIterators
// with simple slice-based iterators.
func analyzeTransactions(txns []CompactTransaction, variableThreshold uint32) *servicepb.AnalyzeTransactionsResponse {
	var totalReverted uint64
	for i := range txns {
		if txns[i].Reverted {
			totalReverted++
		}
	}

	makeIter := func() func() (CompactTransaction, error) {
		i := 0

		return func() (CompactTransaction, error) {
			if i >= len(txns) {
				return CompactTransaction{}, io.EOF
			}
			ct := txns[i]
			i++

			return ct, nil
		}
	}

	count := totalReverted

	resp, err := AnalyzeTransactionsFromIterators(
		makeIter(), makeIter(),
		func() uint64 { return count },
		variableThreshold, nil,
	)
	if err != nil {
		panic(fmt.Sprintf("analyzeTransactions: unexpected error: %v", err))
	}

	return resp
}

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

	resp := analyzeTransactions(nil, 0)
	require.NotNil(t, resp)
	assert.Equal(t, uint64(0), resp.GetTotalTransactions())
	assert.Equal(t, uint64(0), resp.GetTotalReverted())
	assert.Empty(t, resp.GetFlowPatterns())
}

func TestAnalyzeTransactions_SingleSimple(t *testing.T) {
	t.Parallel()

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("world", "bank:main", "USD", 100),
		}),
	}

	resp := analyzeTransactions(txns, 0)
	require.NotNil(t, resp)
	assert.Equal(t, uint64(1), resp.GetTotalTransactions())
	assert.Equal(t, uint64(0), resp.GetTotalReverted())
	require.Len(t, resp.GetFlowPatterns(), 1)

	pattern := resp.GetFlowPatterns()[0]
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE, pattern.GetStructure())
	assert.Equal(t, uint64(1), pattern.GetTransactionCount())
	require.Len(t, pattern.GetPostings(), 1)
	assert.Equal(t, "world", pattern.GetPostings()[0].GetSourcePattern())
	assert.Equal(t, "bank:main", pattern.GetPostings()[0].GetDestinationPattern())
	assert.Equal(t, "USD", pattern.GetPostings()[0].GetAsset())
}

func TestAnalyzeTransactions_MultiDestination(t *testing.T) {
	t.Parallel()

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("bank:main", "bank:fees", "USD", 10),
			makeCompactPosting("bank:main", "users:alice", "USD", 90),
		}),
	}

	resp := analyzeTransactions(txns, 0)
	require.Len(t, resp.GetFlowPatterns(), 1)
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION, resp.GetFlowPatterns()[0].GetStructure())
}

func TestAnalyzeTransactions_MultiSource(t *testing.T) {
	t.Parallel()

	txns := []CompactTransaction{
		makeCompactTransaction(1000000, []CompactPosting{
			makeCompactPosting("users:alice", "bank:main", "USD", 50),
			makeCompactPosting("users:bob", "bank:main", "USD", 50),
		}),
	}

	resp := analyzeTransactions(txns, 0)
	require.Len(t, resp.GetFlowPatterns(), 1)
	assert.Equal(t, servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_SOURCE, resp.GetFlowPatterns()[0].GetStructure())
}

func TestAnalyzeTransactions_NormalizationUUID(t *testing.T) {
	t.Parallel()

	// Create 12 transactions with different UUID user addresses (>10 = default threshold)
	var txns []CompactTransaction

	for i := range 12 {
		uuid := fmt.Sprintf("a0b1c2d3-e4f5-6789-abcd-0000000000%02x", i)
		txns = append(txns, makeCompactTransaction(uint64(1000000+i*1000000),
			[]CompactPosting{
				makeCompactPosting(fmt.Sprintf("users:%s:main", uuid), "bank:fees", "USD", 10),
			},
		))
	}

	resp := analyzeTransactions(txns, 0)
	require.Len(t, resp.GetFlowPatterns(), 1)

	pattern := resp.GetFlowPatterns()[0]
	require.Len(t, pattern.GetPostings(), 1)
	assert.Equal(t, "users:{id}:main", pattern.GetPostings()[0].GetSourcePattern())
	assert.Equal(t, "bank:fees", pattern.GetPostings()[0].GetDestinationPattern())
}

func TestAnalyzeTransactions_NormalizationNumeric(t *testing.T) {
	t.Parallel()

	var txns []CompactTransaction
	for i := range 12 {
		txns = append(txns, makeCompactTransaction(uint64(1000000+i*1000000),
			[]CompactPosting{
				makeCompactPosting(fmt.Sprintf("orders:%d", 1000+i), "bank:revenue", "EUR", 50),
			},
		))
	}

	resp := analyzeTransactions(txns, 0)
	require.Len(t, resp.GetFlowPatterns(), 1)

	pattern := resp.GetFlowPatterns()[0]
	require.Len(t, pattern.GetPostings(), 1)
	assert.Equal(t, "orders:{number}", pattern.GetPostings()[0].GetSourcePattern())
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

	resp := analyzeTransactions(txns, 0)
	require.Len(t, resp.GetFlowPatterns(), 1)

	temporal := resp.GetFlowPatterns()[0].GetTemporal()
	require.NotNil(t, temporal)
	assert.Equal(t, dayMicro*0, temporal.GetFirstSeen().GetData())
	assert.Equal(t, dayMicro*2, temporal.GetLastSeen().GetData())
	assert.InDelta(t, 1.0, temporal.GetTransactionsPerDay(), 0.1)
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

	resp := analyzeTransactions(txns, 0)
	require.Len(t, resp.GetFlowPatterns(), 1)

	volumeStats := resp.GetFlowPatterns()[0].GetVolumeStats()
	require.Len(t, volumeStats, 1)
	assert.Equal(t, "USD", volumeStats[0].GetAsset())
	assert.Equal(t, "400", volumeStats[0].GetTotalVolume())
	assert.Equal(t, "200", volumeStats[0].GetAverageVolume())
	assert.Equal(t, "100", volumeStats[0].GetMinVolume())
	assert.Equal(t, "300", volumeStats[0].GetMaxVolume())
	assert.Equal(t, uint64(2), volumeStats[0].GetTransactionCount())
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

	resp := analyzeTransactions(txns, 0)
	assert.Equal(t, uint64(2), resp.GetTotalTransactions())
	assert.Equal(t, uint64(1), resp.GetTotalReverted())
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

	resp := analyzeTransactions(txns, 0)
	require.Len(t, resp.GetFlowPatterns(), 1)
	assert.Equal(t, []string{"category", "region", "source"}, resp.GetFlowPatterns()[0].GetMetadataKeys())
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

	resp := analyzeTransactions(txns, 0)
	assert.Equal(t, uint64(3), resp.GetTotalTransactions())
	require.Len(t, resp.GetFlowPatterns(), 2)

	// First pattern should have 2 transactions (highest count)
	assert.Equal(t, uint64(2), resp.GetFlowPatterns()[0].GetTransactionCount())
	assert.Equal(t, uint64(1), resp.GetFlowPatterns()[1].GetTransactionCount())
}
