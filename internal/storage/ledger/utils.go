package ledger

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v4/query"
)

func isPartialAddress(address string) bool {
	src := strings.Split(address, ":")

	for index, segment := range src {
		if segment == "" {
			return true
		}
		if segment == "..." && index == len(src)-1 {
			return true
		}
	}

	return false
}

func filterAccountAddress(address, key string) string {
	parts := make([]string, 0)

	if isPartialAddress(address) {
		src := strings.Split(address, ":")
		if src[len(src)-1] != "" {
			parts = append(parts, fmt.Sprintf("jsonb_array_length(%s_array) = %d", key, len(src)))
		}

		for i, segment := range src {
			if len(segment) == 0 || segment == "..." {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s_array @@ ('$[%d] == \"%s\"')::jsonpath", key, i, segment))
		}
	} else {
		parts = append(parts, fmt.Sprintf("%s = '%s'", key, address))
	}

	return strings.Join(parts, " and ")
}

// collectAddressFilters visits all address filter values (without short-circuiting)
// and returns the collected addresses and whether any partial address was found.
func collectAddressFilters(q interface {
	UseFilter(string, ...func(any) bool) bool
}) ([]string, bool) {
	var addresses []string
	var needSegments bool
	q.UseFilter("address", func(value any) bool {
		switch v := value.(type) {
		case string:
			addresses = append(addresses, v)
			if isPartialAddress(v) {
				needSegments = true
			}
		default:
			// $in operator passes arrays — these are always exact addresses,
			// not partial, so we skip them (no GIN index optimization possible).
		}
		return false
	})
	return addresses, needSegments
}

// applyLateralAddressFilter conditionally pushes the address filter into a
// LATERAL join subquery when canPush is true and there are addresses to filter.
func applyLateralAddressFilter(subQuery *bun.SelectQuery, addresses []string, canPush bool) *bun.SelectQuery {
	if len(addresses) > 0 && canPush {
		subQuery = subQuery.Where(buildAddressFilterForLateral(addresses))
	}
	return subQuery
}

// canPushAddressFilterToLateral checks whether it is safe to push the address
// filter into the LATERAL join by inspecting the query builder's JSON AST.
//
// The optimization is UNSAFE when:
//   - An address filter is inside a $not (at any depth): the lateral keeps
//     matching rows, but the outer WHERE negates them → 0 results.
//   - An address filter is inside a $or that also has branches without address
//     filters: the lateral excludes rows matching the non-address branch → missing results.
//
// The optimization is SAFE when:
//   - $not only wraps non-address filters (e.g. metadata)
//   - $or wraps a single item (no-op wrapper)
//   - $or branches all contain address filters
//   - Pure $and at any depth
func canPushAddressFilterToLateral(builder query.Builder) bool {
	if builder == nil {
		return true
	}
	data, err := json.Marshal(builder)
	if err != nil {
		return false
	}
	var node map[string]any
	if err := json.Unmarshal(data, &node); err != nil {
		return false
	}
	return isNodeSafeForLateral(node, false)
}

// isAddressKey returns true if the key refers to an address/account field.
func isAddressKey(key string) bool {
	return key == "address" || key == "account"
}

// isLeafOperator returns true for query leaf operators ($match, $gt, etc).
// Must match leaf operators in go-libs/v3/query/expression.go (mapMapToExpression).
func isLeafOperator(op string) bool {
	switch op {
	case "$match", "$gt", "$gte", "$lt", "$lte", "$like", "$exists", "$in":
		return true
	}
	return false
}

// nodeContainsAddressFilter checks whether a JSON AST subtree contains
// any filter on an address/account field.
func nodeContainsAddressFilter(node map[string]any) bool {
	for op, value := range node {
		switch {
		case isLeafOperator(op):
			if m, ok := value.(map[string]any); ok {
				for key := range m {
					if isAddressKey(key) {
						return true
					}
				}
			}
		case op == "$not":
			if child, ok := value.(map[string]any); ok {
				if nodeContainsAddressFilter(child) {
					return true
				}
			}
		case op == "$and" || op == "$or":
			if items, ok := value.([]any); ok {
				for _, item := range items {
					if child, ok := item.(map[string]any); ok {
						if nodeContainsAddressFilter(child) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// isNodeSafeForLateral walks the JSON AST and checks whether pushing the
// address filter into the LATERAL join would produce correct results.
// insideNot tracks whether we are inside a $not ancestor.
func isNodeSafeForLateral(node map[string]any, insideNot bool) bool {
	for op, value := range node {
		switch {
		case isLeafOperator(op):
			// Address filter inside $not → unsafe
			if insideNot {
				if m, ok := value.(map[string]any); ok {
					for key := range m {
						if isAddressKey(key) {
							return false
						}
					}
				}
			}

		case op == "$not":
			if child, ok := value.(map[string]any); ok {
				if !isNodeSafeForLateral(child, true) {
					return false
				}
			}

		case op == "$and":
			if items, ok := value.([]any); ok {
				for _, item := range items {
					if child, ok := item.(map[string]any); ok {
						if !isNodeSafeForLateral(child, insideNot) {
							return false
						}
					}
				}
			}

		case op == "$or":
			items, ok := value.([]any)
			if !ok {
				continue
			}

			if insideNot {
				// NOT(A OR B) = NOT A AND NOT B — if any branch contains
				// an address filter, that becomes a negated address → unsafe.
				// nodeContainsAddressFilter already checks the full subtree,
				// so no further recursion is needed.
				for _, item := range items {
					if child, ok := item.(map[string]any); ok {
						if nodeContainsAddressFilter(child) {
							return false
						}
					}
				}
				continue
			}

			if len(items) > 1 {
				// $or with multiple branches: unsafe if it mixes branches
				// that contain address filters with branches that don't.
				hasAddr := false
				hasNonAddr := false
				for _, item := range items {
					if child, ok := item.(map[string]any); ok {
						if nodeContainsAddressFilter(child) {
							hasAddr = true
						} else {
							hasNonAddr = true
						}
					}
				}
				if hasAddr && hasNonAddr {
					return false
				}
			}

			// Recurse into children for nested issues
			for _, item := range items {
				if child, ok := item.(map[string]any); ok {
					if !isNodeSafeForLateral(child, insideNot) {
						return false
					}
				}
			}
		}
	}
	return true
}

// buildAddressFilterForLateral builds an OR condition of all address filters
// to push into a LATERAL join, allowing Postgres to use the GIN index on address_array.
func buildAddressFilterForLateral(addresses []string) string {
	if len(addresses) == 1 {
		return filterAccountAddress(addresses[0], "address")
	}
	conditions := make([]string, len(addresses))
	for i, addr := range addresses {
		conditions[i] = "(" + filterAccountAddress(addr, "address") + ")"
	}
	return strings.Join(conditions, " OR ")
}

func explodeAddress(address string) map[string]any {
	parts := strings.Split(address, ":")
	ret := make(map[string]any, len(parts)+1)
	for i, part := range parts {
		ret[fmt.Sprint(i)] = part
	}
	ret[fmt.Sprint(len(parts))] = nil

	return ret
}
