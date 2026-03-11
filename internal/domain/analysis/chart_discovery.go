package analysis

import (
	"errors"
	"fmt"
	"io"
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
	uuidPattern     = `^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`
	numericPattern  = `^[0-9]+$`
	alphanumPattern = `^[a-zA-Z0-9_-]+$`
)

// isUUID checks if s matches the UUID pattern without using regexp.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}

	for i := range 36 {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if s[i] != '-' {
				return false
			}
		} else {
			c := s[i]
			if (c < 'a' || c > 'f') && (c < '0' || c > '9') {
				return false
			}
		}
	}

	return true
}

// isNumeric checks if s matches ^[0-9]+$ without using regexp.
func isNumeric(s string) bool {
	if len(s) == 0 {
		return false
	}

	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

// isAlphanumeric checks if s matches ^[a-zA-Z0-9_-]+$ without using regexp.
func isAlphanumeric(s string) bool {
	if len(s) == 0 {
		return false
	}

	for i := range len(s) {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}

	return true
}

// trieNode represents a single node in the address trie.
type trieNode struct {
	children       map[string]*trieNode
	terminating    int       // number of accounts that terminate at this node
	assets         []string  // distinct assets for accounts terminating here
	metadataKeys   []string  // distinct metadata keys for accounts terminating here
	mergedChild    *trieNode // overflow subtree (chart) or merged variable node (flow normalization)
	overflowCount  int       // number of distinct children routed to mergedChild overflow
	varPlaceholder string    // e.g. "{id}" — set by precomputeNormalization for flow discovery
}

func newTrieNode() *trieNode {
	return &trieNode{
		children: make(map[string]*trieNode),
	}
}

// progressReportInterval is the number of items between progress callbacks.
const progressReportInterval = 500

// Analyze scans a slice of compact accounts and returns an AnalyzeAccountsResponse with
// a suggested ChartOfAccounts, discovered patterns, and total account count.
func Analyze(accounts []CompactAccount, variableThreshold uint32) *servicepb.AnalyzeAccountsResponse {
	i := 0
	next := func() (CompactAccount, error) {
		if i >= len(accounts) {
			return CompactAccount{}, io.EOF
		}

		acc := accounts[i]
		i++

		return acc, nil
	}

	resp, err := AnalyzeFromIterator(next, variableThreshold, nil)
	if err != nil {
		// Slice iterator never returns a non-EOF error.
		panic(fmt.Sprintf("unexpected error from slice iterator: %v", err))
	}

	return resp
}

// AnalyzeFromIterator incrementally builds a trie from accounts yielded by next.
// Each account is discarded after insertion, so memory is O(unique address segments)
// instead of O(N accounts).
// If onProgress is non-nil, it is called periodically with (processed, total) counts.
func AnalyzeFromIterator(next func() (CompactAccount, error), variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
	if variableThreshold == 0 {
		variableThreshold = DefaultVariableThreshold
	}

	// Cap children per node: once reached, new unseen segments spill into mergedChild.
	childCap := int(variableThreshold) * 2

	root := newTrieNode()

	var totalAccounts uint64

	for {
		acc, err := next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("reading account for analysis: %w", err)
		}

		totalAccounts++
		if onProgress != nil && totalAccounts%progressReportInterval == 0 {
			onProgress(totalAccounts, 0)
		}

		segments := strings.Split(acc.Address, ":")

		node := root
		for _, seg := range segments {
			child, ok := node.children[seg]
			if ok {
				node = child

				continue
			}

			// New segment: check if we've hit the cap.
			if len(node.children) >= childCap {
				// Route into the overflow subtree.
				if node.mergedChild == nil {
					node.mergedChild = newTrieNode()
				}

				node.overflowCount++
				node = node.mergedChild

				continue
			}

			child = newTrieNode()
			node.children[seg] = child
			node = child
		}

		node.terminating++
		node.assets = mergeDistinct(node.assets, acc.Assets)
		node.metadataKeys = mergeDistinct(node.metadataKeys, acc.MetadataKeys)
	}

	if totalAccounts == 0 {
		return &servicepb.AnalyzeAccountsResponse{
			SuggestedChart: &commonpb.ChartOfAccounts{},
		}, nil
	}

	// Extract patterns first — classifyChildren mutates shared trie nodes via mergeTrieNodes.
	var patterns []*servicepb.AccountPattern
	extractPatterns(root, nil, nil, variableThreshold, &patterns)

	// Convert trie to chart tree (may mutate trie nodes).
	roots, _ := classifyChildren(root, variableThreshold)

	return &servicepb.AnalyzeAccountsResponse{
		SuggestedChart: &commonpb.ChartOfAccounts{
			Roots: roots,
		},
		Patterns:      patterns,
		TotalAccounts: totalAccounts,
	}, nil
}

// childInfo holds information about a trie node's child during classification.
type childInfo struct {
	key   string
	node  *trieNode
	count int
}

// classifyChildren converts a trie node's children into a map of fixed ChartSegments
// and an optional ChartVariable for the variable portion.
// If the number of distinct children exceeds the threshold, children are merged
// into a single variable segment. When there's a mix of common fixed values and
// many unique values, the common ones stay fixed and the rest become variable.
func classifyChildren(node *trieNode, threshold uint32) (map[string]*commonpb.ChartSegment, *commonpb.ChartVariable) {
	if len(node.children) == 0 && node.mergedChild == nil {
		return nil, nil
	}

	// Count total accounts under each child (recursive)
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

	// Effective distinct children = stored children + overflow count.
	effectiveCount := uint32(len(infos)) + uint32(node.overflowCount)

	if effectiveCount <= threshold {
		// All children are fixed
		return buildFixedMap(infos, threshold), nil
	}

	// Too many children: check for mixed case
	// Heuristic: children appearing more than once or matching a "common" pattern
	// stay as fixed; the rest become variable.
	totalChildAccounts := 0
	for _, info := range infos {
		totalChildAccounts += info.count
	}

	if node.mergedChild != nil {
		totalChildAccounts += countAccounts(node.mergedChild)
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

	// Account for overflow children in the variable count.
	effectiveVariableCount := uint32(len(variableInfos)) + uint32(node.overflowCount)

	// If all would be variable, just make one variable segment
	if len(fixedInfos) == 0 || effectiveVariableCount <= threshold {
		// Either all are clearly variable, or the "variable" set is small enough
		// to actually all be fixed — reconsider
		if effectiveVariableCount <= threshold && len(fixedInfos) > 0 {
			// The variable set is small, keep them all fixed
			return buildFixedMap(infos, threshold), nil
		}
	}

	// Build merged variable node: merge all variable children's sub-trees
	mergedVariableNode := newTrieNode()

	variableKeys := make([]string, 0, len(variableInfos))
	for _, info := range variableInfos {
		variableKeys = append(variableKeys, info.key)
		mergeTrieNodes(mergedVariableNode, info.node)
	}

	// Merge the overflow subtree into the variable node.
	if node.mergedChild != nil {
		mergeTrieNodes(mergedVariableNode, node.mergedChild)
	}

	varChildren, varVariable := classifyChildren(mergedVariableNode, threshold)
	chartVar := &commonpb.ChartVariable{
		Name:     inferVariableName(variableKeys),
		Pattern:  inferPattern(variableKeys),
		Account:  mergedVariableNode.terminating > 0,
		Children: varChildren,
		Variable: varVariable,
	}

	// Add fixed segments
	var fixedMap map[string]*commonpb.ChartSegment
	if len(fixedInfos) > 0 {
		fixedMap = buildFixedMap(fixedInfos, threshold)
	}

	return fixedMap, chartVar
}

// buildFixedMap creates a map of fixed ChartSegments from classified child nodes.
func buildFixedMap(infos []childInfo, threshold uint32) map[string]*commonpb.ChartSegment {
	m := make(map[string]*commonpb.ChartSegment, len(infos))
	for _, info := range infos {
		children, variable := classifyChildren(info.node, threshold)
		m[info.key] = &commonpb.ChartSegment{
			Account:  info.node.terminating > 0,
			Children: children,
			Variable: variable,
		}
	}

	return m
}

// mergeTrieNodes merges src's children into dst.
func mergeTrieNodes(dst, src *trieNode) {
	dst.terminating += src.terminating
	dst.assets = mergeDistinct(dst.assets, src.assets)
	dst.metadataKeys = mergeDistinct(dst.metadataKeys, src.metadataKeys)
	dst.overflowCount += src.overflowCount

	for key, srcChild := range src.children {
		if dstChild, ok := dst.children[key]; ok {
			mergeTrieNodes(dstChild, srcChild)
		} else {
			dst.children[key] = srcChild
		}
	}

	if src.mergedChild != nil {
		if dst.mergedChild == nil {
			dst.mergedChild = src.mergedChild
		} else {
			mergeTrieNodes(dst.mergedChild, src.mergedChild)
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

	if len(node.children) == 0 && node.mergedChild == nil {
		return
	}

	position := uint32(len(pathParts))

	// Effective distinct children = stored + overflow.
	effectiveCount := uint32(len(node.children)) + uint32(node.overflowCount)

	if effectiveCount <= threshold {
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

	// Variable node: merge all children + overflow
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
		UniqueValues:    uint64(effectiveCount),
		Examples:        examples,
	}

	merged := newTrieNode()
	for _, child := range node.children {
		mergeTrieNodes(merged, child)
	}

	if node.mergedChild != nil {
		mergeTrieNodes(merged, node.mergedChild)
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
		if !isUUID(v) {
			allUUID = false
		}

		if !isNumeric(v) {
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
		if !isAlphanumeric(v) {
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
		if !isUUID(v) {
			allUUID = false
		}

		if !isNumeric(v) {
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
	if isUUID(key) {
		return false
	}

	if isNumeric(key) && len(key) > 3 {
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

	if node.mergedChild != nil {
		total += countAccounts(node.mergedChild)
	}

	return total
}

// collectAssets extracts distinct asset names from an account's volumes.
func collectAssets(acc *commonpb.Account) []string {
	if len(acc.GetVolumes()) == 0 {
		return nil
	}

	assets := make([]string, 0, len(acc.GetVolumes()))
	for asset := range acc.GetVolumes() {
		assets = append(assets, asset)
	}

	sort.Strings(assets)

	return assets
}

// collectMetadataKeys extracts metadata keys from an account.
func collectMetadataKeys(acc *commonpb.Account) []string {
	if acc.GetMetadata() == nil || len(acc.GetMetadata().GetMetadata()) == 0 {
		return nil
	}

	keys := make([]string, 0, len(acc.GetMetadata().GetMetadata()))
	for _, m := range acc.GetMetadata().GetMetadata() {
		keys = append(keys, m.GetKey())
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

// SuggestAccountTypes converts discovered patterns from an analysis result into
// a slice of AccountType suggestions. Each pattern becomes an account type with
// an auto-generated name derived from its fixed segments.
func SuggestAccountTypes(resp *servicepb.AnalyzeAccountsResponse) []*commonpb.AccountType {
	if resp == nil || len(resp.GetPatterns()) == 0 {
		return nil
	}

	seen := make(map[string]int) // track name collisions
	types := make([]*commonpb.AccountType, 0, len(resp.GetPatterns()))

	for _, p := range resp.GetPatterns() {
		name := deriveTypeName(p)

		// Handle name collisions by appending a suffix.
		if count, ok := seen[name]; ok {
			seen[name] = count + 1
			name = fmt.Sprintf("%s-%d", name, count+1)
		} else {
			seen[name] = 1
		}

		types = append(types, &commonpb.AccountType{
			Name:            name,
			Pattern:         p.GetPattern(),
			Status:          commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
			EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
		})
	}

	return types
}

// deriveTypeName generates a human-readable name from pattern segments.
// Fixed segments are joined with hyphens; variable segments are omitted.
// Example: "users:{id}:checking" → "users-checking".
func deriveTypeName(p *servicepb.AccountPattern) string {
	parts := strings.Split(p.GetPattern(), ":")
	var fixed []string
	for _, part := range parts {
		if len(part) > 0 && part[0] != '{' {
			fixed = append(fixed, part)
		}
	}
	if len(fixed) == 0 {
		return "default"
	}

	return strings.Join(fixed, "-")
}
