package analysis

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// DefaultVariableThreshold is the maximum number of distinct children at a trie
// node before the node is classified as a variable segment.
const DefaultVariableThreshold = 10

// maxExamples is the maximum number of example values included per pattern segment.
const maxExamples = 5

var (
	uuidRegex        = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
	numericRegex     = regexp.MustCompile(`^[0-9]+$`)
	alphanumRegex    = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	uuidPattern      = `^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`
	numericPattern   = `^[0-9]+$`
	alphanumPattern  = `^[a-zA-Z0-9_-]+$`
)

// trieNode represents a single node in the address trie.
type trieNode struct {
	children     map[string]*trieNode
	terminating  int      // number of accounts that terminate at this node
	assets       []string // distinct assets for accounts terminating here
	metadataKeys []string // distinct metadata keys for accounts terminating here
}

func newTrieNode() *trieNode {
	return &trieNode{
		children: make(map[string]*trieNode),
	}
}

// Analyze scans a slice of compact accounts and returns an AnalyzeAccountsResponse with
// a suggested ChartOfAccounts, discovered patterns, and total account count.
func Analyze(accounts []CompactAccount, variableThreshold uint32) *servicepb.AnalyzeAccountsResponse {
	if variableThreshold == 0 {
		variableThreshold = DefaultVariableThreshold
	}

	if len(accounts) == 0 {
		return &servicepb.AnalyzeAccountsResponse{
			SuggestedChart: &commonpb.ChartOfAccounts{},
		}
	}

	// Build trie
	root := newTrieNode()
	for _, acc := range accounts {
		segments := strings.Split(acc.Address, ":")
		node := root
		for _, seg := range segments {
			child, ok := node.children[seg]
			if !ok {
				child = newTrieNode()
				node.children[seg] = child
			}
			node = child
		}
		// Mark terminating node with account data
		node.terminating++
		node.assets = mergeDistinct(node.assets, acc.Assets)
		node.metadataKeys = mergeDistinct(node.metadataKeys, acc.MetadataKeys)
	}

	// Convert trie to chart segments
	chartSegments := classifyChildren(root, variableThreshold)

	// Extract patterns
	var patterns []*servicepb.AccountPattern
	extractPatterns(root, nil, nil, variableThreshold, &patterns)

	return &servicepb.AnalyzeAccountsResponse{
		SuggestedChart: &commonpb.ChartOfAccounts{
			Segments: chartSegments,
		},
		Patterns:      patterns,
		TotalAccounts: uint64(len(accounts)),
	}
}

// classifyChildren converts a trie node's children into ChartSegment protos.
// If the number of distinct children exceeds the threshold, children are merged
// into a single variable segment. When there's a mix of common fixed values and
// many unique values, the common ones stay fixed and the rest become variable.
func classifyChildren(node *trieNode, threshold uint32) []*commonpb.ChartSegment {
	if len(node.children) == 0 {
		return nil
	}

	// Count total accounts under each child (recursive)
	type childInfo struct {
		key   string
		node  *trieNode
		count int
	}
	var infos []childInfo
	for key, child := range node.children {
		infos = append(infos, childInfo{key: key, node: child, count: countAccounts(child)})
	}

	// Sort by count descending for deterministic ordering
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].count != infos[j].count {
			return infos[i].count > infos[j].count
		}
		return infos[i].key < infos[j].key
	})

	if uint32(len(infos)) <= threshold {
		// All children are fixed
		var segments []*commonpb.ChartSegment
		// Sort alphabetically for output stability
		sort.Slice(infos, func(i, j int) bool { return infos[i].key < infos[j].key })
		for _, info := range infos {
			segments = append(segments, &commonpb.ChartSegment{
				FixedValue: info.key,
				Children:   classifyChildren(info.node, threshold),
			})
		}
		return segments
	}

	// Too many children: check for mixed case
	// Heuristic: children appearing more than once or matching a "common" pattern
	// stay as fixed; the rest become variable.
	// Simple approach: if a child key contains only letters/underscores and has
	// significant presence (>= 1% of total or appears multiple times in tree), keep as fixed.
	totalChildAccounts := 0
	for _, info := range infos {
		totalChildAccounts += info.count
	}

	var (
		fixedInfos    []childInfo
		variableInfos []childInfo
	)

	// Threshold for considering a child "common enough" to be fixed in mixed mode:
	// it must have at least 2% of accounts or appear with further sub-structure.
	mixedThreshold := max(totalChildAccounts/50, 1)

	for _, info := range infos {
		if info.count >= mixedThreshold && isLikelyFixedName(info.key) {
			fixedInfos = append(fixedInfos, info)
		} else {
			variableInfos = append(variableInfos, info)
		}
	}

	// If all would be variable, just make one variable segment
	if len(fixedInfos) == 0 || uint32(len(variableInfos)) <= threshold {
		// Either all are clearly variable, or the "variable" set is small enough
		// to actually all be fixed — reconsider
		if uint32(len(variableInfos)) <= threshold && len(fixedInfos) > 0 {
			// The variable set is small, keep them all fixed
			sort.Slice(infos, func(i, j int) bool { return infos[i].key < infos[j].key })
			var segments []*commonpb.ChartSegment
			for _, info := range infos {
				segments = append(segments, &commonpb.ChartSegment{
					FixedValue: info.key,
					Children:   classifyChildren(info.node, threshold),
				})
			}
			return segments
		}
	}

	// Build merged variable node: merge all variable children's sub-trees
	mergedVariableNode := newTrieNode()
	variableKeys := make([]string, 0, len(variableInfos))
	for _, info := range variableInfos {
		variableKeys = append(variableKeys, info.key)
		mergeTrieNodes(mergedVariableNode, info.node)
	}

	varName := inferVariableName(variableKeys)
	varPattern := inferPattern(variableKeys)

	var segments []*commonpb.ChartSegment

	// Add fixed segments first (sorted)
	sort.Slice(fixedInfos, func(i, j int) bool { return fixedInfos[i].key < fixedInfos[j].key })
	for _, info := range fixedInfos {
		segments = append(segments, &commonpb.ChartSegment{
			FixedValue: info.key,
			Children:   classifyChildren(info.node, threshold),
		})
	}

	// Add variable segment
	segments = append(segments, &commonpb.ChartSegment{
		Variable: &commonpb.ChartVariable{
			Name:            varName,
			InferredPattern: varPattern,
		},
		Children: classifyChildren(mergedVariableNode, threshold),
	})

	return segments
}

// mergeTrieNodes merges src's children into dst.
func mergeTrieNodes(dst, src *trieNode) {
	dst.terminating += src.terminating
	dst.assets = mergeDistinct(dst.assets, src.assets)
	dst.metadataKeys = mergeDistinct(dst.metadataKeys, src.metadataKeys)
	for key, srcChild := range src.children {
		if dstChild, ok := dst.children[key]; ok {
			mergeTrieNodes(dstChild, srcChild)
		} else {
			dst.children[key] = srcChild
		}
	}
}

// extractPatterns walks the trie and emits one AccountPattern per leaf path.
func extractPatterns(node *trieNode, pathParts []string, pathSegments []*servicepb.PatternSegment, threshold uint32, out *[]*servicepb.AccountPattern) {
	if node.terminating > 0 {
		pattern := strings.Join(pathParts, ":")
		*out = append(*out, &servicepb.AccountPattern{
			Pattern:      pattern,
			AccountCount: uint64(node.terminating),
			Assets:       sortedCopy(node.assets),
			MetadataKeys: sortedCopy(node.metadataKeys),
			Segments:     cloneSegments(pathSegments),
		})
	}

	if len(node.children) == 0 {
		return
	}

	position := uint32(len(pathParts))

	if uint32(len(node.children)) <= threshold {
		// All children are fixed
		keys := sortedKeys(node.children)
		for _, key := range keys {
			child := node.children[key]
			seg := &servicepb.PatternSegment{
				Position:     position,
				Type:         servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_FIXED,
				FixedValue:   key,
				UniqueValues: 1,
				Examples:     []string{key},
			}
			extractPatterns(child,
				append(pathParts, key),
				append(pathSegments, seg),
				threshold, out)
		}
		return
	}

	// Variable node: merge all children
	allKeys := sortedKeys(node.children)
	examples := allKeys
	if len(examples) > maxExamples {
		examples = examples[:maxExamples]
	}

	varName := inferVariableName(allKeys)
	varPattern := inferPattern(allKeys)

	seg := &servicepb.PatternSegment{
		Position:        position,
		Type:            servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE,
		VariableName:    varName,
		InferredPattern: varPattern,
		UniqueValues:    uint64(len(allKeys)),
		Examples:        examples,
	}

	merged := newTrieNode()
	for _, child := range node.children {
		mergeTrieNodes(merged, child)
	}

	extractPatterns(merged,
		append(pathParts, fmt.Sprintf("{%s}", varName)),
		append(pathSegments, seg),
		threshold, out)
}

// inferPattern determines the regex pattern that best describes the given values.
func inferPattern(values []string) string {
	if len(values) == 0 {
		return ""
	}

	allUUID := true
	allNumeric := true

	for _, v := range values {
		if !uuidRegex.MatchString(v) {
			allUUID = false
		}
		if !numericRegex.MatchString(v) {
			allNumeric = false
		}
		if !allUUID && !allNumeric {
			break
		}
	}

	if allUUID {
		return uuidPattern
	}
	if allNumeric {
		return numericPattern
	}

	allAlphanum := true
	for _, v := range values {
		if !alphanumRegex.MatchString(v) {
			allAlphanum = false
			break
		}
	}
	if allAlphanum {
		return alphanumPattern
	}

	return ""
}

// inferVariableName produces a human-readable name for a variable segment.
func inferVariableName(values []string) string {
	if len(values) == 0 {
		return "id"
	}

	allUUID := true
	allNumeric := true
	for _, v := range values {
		if !uuidRegex.MatchString(v) {
			allUUID = false
		}
		if !numericRegex.MatchString(v) {
			allNumeric = false
		}
		if !allUUID && !allNumeric {
			break
		}
	}

	if allUUID {
		return "id"
	}
	if allNumeric {
		return "number"
	}
	return "value"
}

// isLikelyFixedName returns true if the key looks like a human-written label
// (all lowercase letters, underscores, hyphens — not a UUID or numeric ID).
func isLikelyFixedName(key string) bool {
	if len(key) == 0 {
		return false
	}
	// UUIDs and long numeric strings are not fixed names
	if uuidRegex.MatchString(key) {
		return false
	}
	if numericRegex.MatchString(key) && len(key) > 3 {
		return false
	}
	// Must start with a letter
	if key[0] < 'a' || key[0] > 'z' {
		if key[0] < 'A' || key[0] > 'Z' {
			return false
		}
	}
	return true
}

// countAccounts returns the total number of terminating accounts in the subtree.
func countAccounts(node *trieNode) int {
	total := node.terminating
	for _, child := range node.children {
		total += countAccounts(child)
	}
	return total
}

// collectAssets extracts distinct asset names from an account's volumes.
func collectAssets(acc *commonpb.Account) []string {
	if len(acc.Volumes) == 0 {
		return nil
	}
	assets := make([]string, 0, len(acc.Volumes))
	for asset := range acc.Volumes {
		assets = append(assets, asset)
	}
	sort.Strings(assets)
	return assets
}

// collectMetadataKeys extracts metadata keys from an account.
func collectMetadataKeys(acc *commonpb.Account) []string {
	if acc.Metadata == nil || len(acc.Metadata.Metadata) == 0 {
		return nil
	}
	keys := make([]string, 0, len(acc.Metadata.Metadata))
	for _, m := range acc.Metadata.Metadata {
		keys = append(keys, m.Key)
	}
	sort.Strings(keys)
	return keys
}

// mergeDistinct merges two sorted slices, returning distinct sorted values.
func mergeDistinct(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	set := make(map[string]struct{}, len(a)+len(b))
	for _, v := range a {
		set[v] = struct{}{}
	}
	for _, v := range b {
		set[v] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for v := range set {
		result = append(result, v)
	}
	sort.Strings(result)
	return result
}

func sortedKeys(m map[string]*trieNode) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedCopy(s []string) []string {
	if s == nil {
		return nil
	}
	c := make([]string, len(s))
	copy(c, s)
	sort.Strings(c)
	return c
}

func cloneSegments(segs []*servicepb.PatternSegment) []*servicepb.PatternSegment {
	if segs == nil {
		return nil
	}
	c := make([]*servicepb.PatternSegment, len(segs))
	copy(c, segs)
	return c
}
