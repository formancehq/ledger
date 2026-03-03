package analysis

import (
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// AnalyzeTransactions scans a slice of compact transactions and returns an AnalyzeTransactionsResponse
// with discovered flow patterns, temporal stats, volume stats, and metadata keys.
func AnalyzeTransactions(txns []CompactTransaction, variableThreshold uint32) *servicepb.AnalyzeTransactionsResponse {
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
	resp, err := AnalyzeTransactionsFromIterators(makeIter(), makeIter(), func() uint64 { return count }, variableThreshold)
	if err != nil {
		// Slice iterators never return a non-EOF error.
		panic(fmt.Sprintf("unexpected error from slice iterator: %v", err))
	}
	return resp
}

// txGroupAccum accumulates statistics for a group of transactions sharing
// the same flow signature. It uses online accumulators instead of storing
// the full transaction list, keeping memory at O(unique signatures).
type txGroupAccum struct {
	signature string
	postings  []*servicepb.NormalizedPosting
	structure servicepb.PostingStructure
	count     uint64
	// Temporal accumulators
	firstSeen, lastSeen uint64
	hasSeen             bool
	hours               [24]uint64
	// Volume accumulators per asset
	volumes map[string]*assetAccum
	// Metadata keys (accumulated)
	metadataKeys []string
}

// assetAccum accumulates volume statistics for a single asset.
type assetAccum struct {
	total *big.Int
	min   *big.Int
	max   *big.Int
	count uint64
}

// addTransaction updates all accumulators with data from a single transaction.
func (g *txGroupAccum) addTransaction(ct CompactTransaction) {
	g.count++

	// Temporal
	if ct.HasTimestamp {
		ts := ct.Timestamp
		if !g.hasSeen || ts < g.firstSeen {
			g.firstSeen = ts
		}
		if !g.hasSeen || ts > g.lastSeen {
			g.lastSeen = ts
		}
		g.hasSeen = true
		hour := time.UnixMicro(int64(ts)).UTC().Hour()
		g.hours[hour]++
	}

	// Volumes
	for j := range ct.Postings {
		p := &ct.Postings[j]
		acc, ok := g.volumes[p.Asset]
		if !ok {
			acc = &assetAccum{
				total: new(big.Int),
				min:   new(big.Int).Set(p.Amount),
				max:   new(big.Int).Set(p.Amount),
			}
			g.volumes[p.Asset] = acc
		}
		acc.total.Add(acc.total, p.Amount)
		acc.count++
		if p.Amount.Cmp(acc.min) < 0 {
			acc.min.Set(p.Amount)
		}
		if p.Amount.Cmp(acc.max) > 0 {
			acc.max.Set(p.Amount)
		}
	}

	// Metadata keys
	if len(ct.MetadataKeys) > 0 {
		g.metadataKeys = mergeDistinct(g.metadataKeys, ct.MetadataKeys)
	}
}

// toFlowPattern materializes the accumulated statistics into a FlowPattern.
func (g *txGroupAccum) toFlowPattern() *servicepb.FlowPattern {
	pattern := &servicepb.FlowPattern{
		Signature:        g.signature,
		Structure:        g.structure,
		TransactionCount: g.count,
		Postings:         g.postings,
		MetadataKeys:     g.metadataKeys,
	}

	// Temporal stats
	if g.count > 0 {
		stats := &servicepb.TemporalStats{}
		if g.hasSeen {
			stats.FirstSeen = &commonpb.Timestamp{Data: g.firstSeen}
			stats.LastSeen = &commonpb.Timestamp{Data: g.lastSeen}

			first := time.UnixMicro(int64(g.firstSeen))
			last := time.UnixMicro(int64(g.lastSeen))
			daySpan := last.Sub(first).Hours() / 24
			if daySpan < 1 {
				daySpan = 1
			}
			stats.TransactionsPerDay = float64(g.count) / daySpan
		}
		for h := 0; h < 24; h++ {
			if g.hours[h] > 0 {
				stats.PeakHours = append(stats.PeakHours, &servicepb.HourBucket{
					Hour:  uint32(h),
					Count: g.hours[h],
				})
			}
		}
		pattern.Temporal = stats
	}

	// Volume stats
	var volumeStats []*servicepb.AssetVolumeStats
	for asset, acc := range g.volumes {
		avg := new(big.Int)
		if acc.count > 0 {
			avg.Div(acc.total, big.NewInt(int64(acc.count)))
		}
		volumeStats = append(volumeStats, &servicepb.AssetVolumeStats{
			Asset:            asset,
			TotalVolume:      acc.total.String(),
			AverageVolume:    avg.String(),
			MinVolume:        acc.min.String(),
			MaxVolume:        acc.max.String(),
			TransactionCount: acc.count,
		})
	}
	sort.Slice(volumeStats, func(i, j int) bool {
		return volumeStats[i].Asset < volumeStats[j].Asset
	})
	pattern.VolumeStats = volumeStats

	return pattern
}

// AnalyzeTransactionsFromIterators performs two-pass streaming analysis of transactions.
// Pass 1 (pass1): builds the address trie and counts totalTransactions.
// Pass 2 (pass2): normalizes postings and aggregates statistics incrementally.
// totalReverted is provided externally (e.g. counted during the log scan) because
// the streaming path cannot retroactively mark already-yielded transactions as reverted.
// Each transaction is discarded after processing, so memory is
// O(unique address segments + unique flow signatures) instead of O(N transactions).
func AnalyzeTransactionsFromIterators(
	pass1 func() (CompactTransaction, error),
	pass2 func() (CompactTransaction, error),
	revertedCount func() uint64,
	variableThreshold uint32,
) (*servicepb.AnalyzeTransactionsResponse, error) {
	if variableThreshold == 0 {
		variableThreshold = DefaultVariableThreshold
	}

	// Pass 1: Build address trie + count totals
	root := newTrieNode()
	var totalTransactions uint64
	for {
		ct, err := pass1()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("pass 1 (trie building): %w", err)
		}
		totalTransactions++
		for j := range ct.Postings {
			insertAddress(root, ct.Postings[j].Source)
			insertAddress(root, ct.Postings[j].Destination)
		}
	}

	resp := &servicepb.AnalyzeTransactionsResponse{
		TotalTransactions: totalTransactions,
		TotalReverted:     revertedCount(),
	}
	if totalTransactions == 0 {
		return resp, nil
	}

	// Pass 2: Normalize + aggregate incrementally
	groups := make(map[string]*txGroupAccum)
	for {
		ct, err := pass2()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("pass 2 (aggregation): %w", err)
		}

		normalized := normalizePostings(ct.Postings, root, variableThreshold)
		sig := computeFlowSignature(normalized)

		g, ok := groups[sig]
		if !ok {
			g = &txGroupAccum{
				signature: sig,
				postings:  normalized,
				structure: classifyPostingStructure(normalized),
				volumes:   make(map[string]*assetAccum),
			}
			groups[sig] = g
		}
		g.addTransaction(ct)
	}

	// Materialize flow patterns
	for _, g := range groups {
		resp.FlowPatterns = append(resp.FlowPatterns, g.toFlowPattern())
	}

	// Sort patterns by transaction count descending, then signature for determinism
	sort.Slice(resp.FlowPatterns, func(i, j int) bool {
		if resp.FlowPatterns[i].TransactionCount != resp.FlowPatterns[j].TransactionCount {
			return resp.FlowPatterns[i].TransactionCount > resp.FlowPatterns[j].TransactionCount
		}
		return resp.FlowPatterns[i].Signature < resp.FlowPatterns[j].Signature
	})

	return resp, nil
}

// insertAddress inserts an address into the trie (split by ":").
func insertAddress(root *trieNode, address string) {
	segments := strings.Split(address, ":")
	node := root
	for _, seg := range segments {
		child, ok := node.children[seg]
		if !ok {
			child = newTrieNode()
			node.children[seg] = child
		}
		node = child
	}
	node.terminating++
}

// normalizeAddress replaces variable segments in an address with placeholder names
// using the trie to determine which segments are variable.
func normalizeAddress(address string, root *trieNode, threshold uint32) string {
	segments := strings.Split(address, ":")
	result := make([]string, len(segments))
	node := root
	for i, seg := range segments {
		if node != nil && uint32(len(node.children)) > threshold {
			// Variable segment: infer placeholder name
			keys := sortedKeys(node.children)
			varName := inferVariableName(keys)
			result[i] = fmt.Sprintf("{%s}", varName)
			// Follow merged children for next level
			merged := newTrieNode()
			for _, child := range node.children {
				mergeTrieNodes(merged, child)
			}
			node = merged
		} else {
			result[i] = seg
			if node != nil {
				node = node.children[seg]
			}
		}
	}
	return strings.Join(result, ":")
}

// normalizePostings normalizes all postings of a transaction.
func normalizePostings(postings []CompactPosting, root *trieNode, threshold uint32) []*servicepb.NormalizedPosting {
	normalized := make([]*servicepb.NormalizedPosting, 0, len(postings))
	for i := range postings {
		normalized = append(normalized, &servicepb.NormalizedPosting{
			SourcePattern:      normalizeAddress(postings[i].Source, root, threshold),
			DestinationPattern: normalizeAddress(postings[i].Destination, root, threshold),
			Asset:              postings[i].Asset,
		})
	}
	return normalized
}

// computeFlowSignature returns a canonical, sorted signature string from normalized postings.
func computeFlowSignature(postings []*servicepb.NormalizedPosting) string {
	parts := make([]string, 0, len(postings))
	for _, p := range postings {
		parts = append(parts, fmt.Sprintf("%s->%s[%s]", p.SourcePattern, p.DestinationPattern, p.Asset))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// classifyPostingStructure determines the posting structure type.
func classifyPostingStructure(postings []*servicepb.NormalizedPosting) servicepb.PostingStructure {
	if len(postings) == 1 {
		return servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE
	}

	sources := make(map[string]struct{})
	destinations := make(map[string]struct{})
	for _, p := range postings {
		sources[p.SourcePattern] = struct{}{}
		destinations[p.DestinationPattern] = struct{}{}
	}

	singleSource := len(sources) == 1
	singleDest := len(destinations) == 1

	switch {
	case singleSource && !singleDest:
		return servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION
	case !singleSource && singleDest:
		return servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_SOURCE
	default:
		return servicepb.PostingStructure_POSTING_STRUCTURE_COMPLEX
	}
}

