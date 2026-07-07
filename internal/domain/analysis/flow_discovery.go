package analysis

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

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
			stats.FirstSeen = g.firstSeen
			stats.LastSeen = g.lastSeen

			first := time.UnixMicro(int64(g.firstSeen))
			last := time.UnixMicro(int64(g.lastSeen))

			daySpan := last.Sub(first).Hours() / 24
			if daySpan < 1 {
				daySpan = 1
			}

			stats.TransactionsPerDay = float64(g.count) / daySpan
		}

		for h := range 24 {
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
		return volumeStats[i].GetAsset() < volumeStats[j].GetAsset()
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
// If onProgress is non-nil, it is called periodically with (processed, total) counts.
// During pass1 processed goes 0→totalLogs; during pass2 it goes totalLogs→2*totalLogs.
func AnalyzeTransactionsFromIterators(
	pass1 func() (CompactTransaction, error),
	pass2 func() (CompactTransaction, error),
	revertedCount func() uint64,
	variableThreshold uint32,
	onProgress func(processed, total uint64),
) (*servicepb.AnalyzeTransactionsResponse, error) {
	if variableThreshold == 0 {
		variableThreshold = DefaultVariableThreshold
	}

	// Pass 1: Build address trie + count totals
	root := newTrieNode()

	var (
		totalTransactions uint64
		pass1Processed    uint64
	)

	for {
		ct, err := pass1()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("pass 1 (trie building): %w", err)
		}

		totalTransactions++

		pass1Processed++
		if onProgress != nil && pass1Processed%progressReportInterval == 0 {
			onProgress(pass1Processed, 0)
		}

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

	// Pre-compute normalization info on the trie (once, not per-transaction).
	precomputeNormalization(root, variableThreshold)

	// Pass 2: Normalize + aggregate incrementally
	addrCache := make(map[string]string)
	groups := make(map[string]*txGroupAccum)

	var pass2Processed uint64

	for {
		ct, err := pass2()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("pass 2 (aggregation): %w", err)
		}

		pass2Processed++
		if onProgress != nil && pass2Processed%progressReportInterval == 0 {
			onProgress(pass1Processed+pass2Processed, pass1Processed*2)
		}

		normalized := normalizePostings(ct.Postings, root, addrCache)
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
	sort.Slice(resp.GetFlowPatterns(), func(i, j int) bool {
		if resp.GetFlowPatterns()[i].GetTransactionCount() != resp.GetFlowPatterns()[j].GetTransactionCount() {
			return resp.GetFlowPatterns()[i].GetTransactionCount() > resp.GetFlowPatterns()[j].GetTransactionCount()
		}

		return resp.GetFlowPatterns()[i].GetSignature() < resp.GetFlowPatterns()[j].GetSignature()
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
// using pre-computed normalization info on the trie (see precomputeNormalization).
func normalizeAddress(address string, root *trieNode) string {
	var b strings.Builder
	b.Grow(len(address) + 16)

	node := root
	first := true
	start := 0

	for i := 0; i <= len(address); i++ {
		if i == len(address) || address[i] == ':' {
			if !first {
				b.WriteByte(':')
			}

			first = false
			seg := address[start:i]

			if node != nil && node.varPlaceholder != "" {
				b.WriteString(node.varPlaceholder)
				node = node.mergedChild
			} else {
				b.WriteString(seg)

				if node != nil {
					node = node.children[seg]
				}
			}

			start = i + 1
		}
	}

	return b.String()
}

// cachedNormalizeAddress returns the normalized form of address, using a cache
// to avoid redundant trie traversals for repeated addresses.
func cachedNormalizeAddress(address string, root *trieNode, cache map[string]string) string {
	if cached, ok := cache[address]; ok {
		return cached
	}

	result := normalizeAddress(address, root)
	cache[address] = result

	return result
}

// normalizePostings normalizes all postings of a transaction.
func normalizePostings(postings []CompactPosting, root *trieNode, addrCache map[string]string) []*servicepb.NormalizedPosting {
	normalized := make([]*servicepb.NormalizedPosting, len(postings))
	for i := range postings {
		normalized[i] = &servicepb.NormalizedPosting{
			SourcePattern:      cachedNormalizeAddress(postings[i].Source, root, addrCache),
			DestinationPattern: cachedNormalizeAddress(postings[i].Destination, root, addrCache),
			Asset:              postings[i].Asset,
		}
	}

	return normalized
}

// computeFlowSignature returns a canonical, sorted signature string from normalized postings.
func computeFlowSignature(postings []*servicepb.NormalizedPosting) string {
	if len(postings) == 1 {
		// Fast path: single posting, no sorting needed.
		p := postings[0]

		return p.GetSourcePattern() + "->" + p.GetDestinationPattern() + "[" + p.GetAsset() + "]"
	}

	parts := make([]string, len(postings))
	for i, p := range postings {
		parts[i] = p.GetSourcePattern() + "->" + p.GetDestinationPattern() + "[" + p.GetAsset() + "]"
	}

	sort.Strings(parts)

	return strings.Join(parts, ";")
}

// precomputeNormalization walks the trie once after Pass 1 and caches
// varPlaceholder and mergedChild on each variable node, so that
// normalizeAddress becomes a simple traversal without allocations.
func precomputeNormalization(node *trieNode, threshold uint32) {
	if uint32(len(node.children)) > threshold {
		keys := make([]string, 0, len(node.children))
		for k := range node.children {
			keys = append(keys, k)
		}

		node.varPlaceholder = "{" + inferVariableName(keys) + "}"

		node.mergedChild = newTrieNode()
		for _, child := range node.children {
			mergeTrieNodesStructure(node.mergedChild, child)
		}

		precomputeNormalization(node.mergedChild, threshold)
	} else {
		for _, child := range node.children {
			precomputeNormalization(child, threshold)
		}
	}
}

// mergeTrieNodesStructure merges only the tree structure (children and terminating count),
// always creating new destination nodes to avoid mutating the source trie.
func mergeTrieNodesStructure(dst, src *trieNode) {
	dst.terminating += src.terminating
	for key, srcChild := range src.children {
		dstChild, ok := dst.children[key]
		if !ok {
			dstChild = newTrieNode()
			dst.children[key] = dstChild
		}

		mergeTrieNodesStructure(dstChild, srcChild)
	}
}

// classifyPostingStructure determines the posting structure type.
func classifyPostingStructure(postings []*servicepb.NormalizedPosting) servicepb.PostingStructure {
	if len(postings) == 1 {
		return servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE
	}

	sources := make(map[string]struct{})
	destinations := make(map[string]struct{})

	for _, p := range postings {
		sources[p.GetSourcePattern()] = struct{}{}
		destinations[p.GetDestinationPattern()] = struct{}{}
	}

	singleSource := len(sources) == 1
	singleDestination := len(destinations) == 1

	switch {
	case singleSource && !singleDestination:
		return servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION
	case !singleSource && singleDestination:
		return servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_SOURCE
	default:
		return servicepb.PostingStructure_POSTING_STRUCTURE_COMPLEX
	}
}
