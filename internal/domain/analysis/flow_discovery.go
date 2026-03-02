package analysis

import (
	"fmt"
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
	if variableThreshold == 0 {
		variableThreshold = DefaultVariableThreshold
	}

	resp := &servicepb.AnalyzeTransactionsResponse{
		TotalTransactions: uint64(len(txns)),
	}

	if len(txns) == 0 {
		return resp
	}

	// Phase 1: Build address trie from all posting addresses
	root := buildTrieFromTransactions(txns)

	// Phase 2: Normalize postings and compute signatures
	type txGroup struct {
		signature string
		postings  []*servicepb.NormalizedPosting
		structure servicepb.PostingStructure
		txns      []CompactTransaction
	}
	groups := make(map[string]*txGroup)

	for i := range txns {
		tx := &txns[i]
		if tx.Reverted {
			resp.TotalReverted++
		}

		normalized := normalizePostings(tx.Postings, root, variableThreshold)
		sig := computeFlowSignature(normalized)
		structure := classifyPostingStructure(normalized)

		g, ok := groups[sig]
		if !ok {
			g = &txGroup{
				signature: sig,
				postings:  normalized,
				structure: structure,
			}
			groups[sig] = g
		}
		g.txns = append(g.txns, txns[i])
	}

	// Phase 3: Aggregate per group
	for _, g := range groups {
		pattern := &servicepb.FlowPattern{
			Signature:        g.signature,
			Structure:        g.structure,
			TransactionCount: uint64(len(g.txns)),
			Postings:         g.postings,
			Temporal:         computeTemporalStats(g.txns),
			VolumeStats:      computeVolumeStats(g.txns),
			MetadataKeys:     collectTransactionMetadataKeys(g.txns),
		}
		resp.FlowPatterns = append(resp.FlowPatterns, pattern)
	}

	// Sort patterns by transaction count descending, then signature for determinism
	sort.Slice(resp.FlowPatterns, func(i, j int) bool {
		if resp.FlowPatterns[i].TransactionCount != resp.FlowPatterns[j].TransactionCount {
			return resp.FlowPatterns[i].TransactionCount > resp.FlowPatterns[j].TransactionCount
		}
		return resp.FlowPatterns[i].Signature < resp.FlowPatterns[j].Signature
	})

	return resp
}

// buildTrieFromTransactions collects all source/destination addresses from transactions
// and builds a trie from them.
func buildTrieFromTransactions(txns []CompactTransaction) *trieNode {
	root := newTrieNode()
	for i := range txns {
		for j := range txns[i].Postings {
			insertAddress(root, txns[i].Postings[j].Source)
			insertAddress(root, txns[i].Postings[j].Destination)
		}
	}
	return root
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

// computeTemporalStats computes temporal statistics for a group of compact transactions.
func computeTemporalStats(txns []CompactTransaction) *servicepb.TemporalStats {
	if len(txns) == 0 {
		return nil
	}

	var (
		firstSeen uint64
		lastSeen  uint64
		hasSeen   bool
		hours     [24]uint64
	)

	for i := range txns {
		if !txns[i].HasTimestamp {
			continue
		}
		ts := txns[i].Timestamp
		if !hasSeen || ts < firstSeen {
			firstSeen = ts
		}
		if !hasSeen || ts > lastSeen {
			lastSeen = ts
		}
		hasSeen = true
		hour := time.UnixMicro(int64(ts)).UTC().Hour()
		hours[hour]++
	}

	stats := &servicepb.TemporalStats{}
	if hasSeen {
		stats.FirstSeen = &commonpb.Timestamp{Data: firstSeen}
		stats.LastSeen = &commonpb.Timestamp{Data: lastSeen}
	}

	// Transactions per day
	if hasSeen {
		first := time.UnixMicro(int64(firstSeen))
		last := time.UnixMicro(int64(lastSeen))
		daySpan := last.Sub(first).Hours() / 24
		if daySpan < 1 {
			daySpan = 1
		}
		stats.TransactionsPerDay = float64(len(txns)) / daySpan
	}

	// Peak hours
	for h := 0; h < 24; h++ {
		if hours[h] > 0 {
			stats.PeakHours = append(stats.PeakHours, &servicepb.HourBucket{
				Hour:  uint32(h),
				Count: hours[h],
			})
		}
	}

	return stats
}

// computeVolumeStats computes volume statistics per asset across all compact transactions in a group.
func computeVolumeStats(txns []CompactTransaction) []*servicepb.AssetVolumeStats {
	type assetAccum struct {
		total *big.Int
		min   *big.Int
		max   *big.Int
		count uint64
	}

	accums := make(map[string]*assetAccum)

	for i := range txns {
		for j := range txns[i].Postings {
			p := &txns[i].Postings[j]
			acc, ok := accums[p.Asset]
			if !ok {
				acc = &assetAccum{
					total: new(big.Int),
					min:   new(big.Int).Set(p.Amount),
					max:   new(big.Int).Set(p.Amount),
				}
				accums[p.Asset] = acc
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
	}

	var stats []*servicepb.AssetVolumeStats
	for asset, acc := range accums {
		avg := new(big.Int)
		if acc.count > 0 {
			avg.Div(acc.total, big.NewInt(int64(acc.count)))
		}
		stats = append(stats, &servicepb.AssetVolumeStats{
			Asset:            asset,
			TotalVolume:      acc.total.String(),
			AverageVolume:    avg.String(),
			MinVolume:        acc.min.String(),
			MaxVolume:        acc.max.String(),
			TransactionCount: acc.count,
		})
	}

	// Sort by asset name for determinism
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Asset < stats[j].Asset
	})

	return stats
}

// collectTransactionMetadataKeys collects distinct metadata keys from all compact transactions.
func collectTransactionMetadataKeys(txns []CompactTransaction) []string {
	var allKeys []string
	for i := range txns {
		if len(txns[i].MetadataKeys) == 0 {
			continue
		}
		allKeys = mergeDistinct(allKeys, txns[i].MetadataKeys)
	}
	return allKeys
}
