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
// LATERAL join subquery when it is safe to do so.
func applyLateralAddressFilter(subQuery *bun.SelectQuery, addresses []string, builder query.Builder) *bun.SelectQuery {
	if len(addresses) > 0 && canPushAddressFilterToLateral(builder) {
		subQuery = subQuery.Where(buildAddressFilterForLateral(addresses))
	}
	return subQuery
}

// canPushAddressFilterToLateral checks whether it is safe to push the address
// filter into the LATERAL join by inspecting the query builder's JSON AST.
//
// The optimization is UNSAFE when:
//   - $not is present: the lateral keeps matching rows, but the outer WHERE
//     negates them → 0 results.
//   - $or is present: the lateral excludes rows that don't match the address
//     filter, but those rows might match another branch of the $or (e.g.
//     a balance or metadata condition) → missing results.
//
// With pure $and queries, the optimization is safe because all conditions
// must be true, so pre-filtering by address is a valid subset operation.
func canPushAddressFilterToLateral(builder query.Builder) bool {
	if builder == nil {
		return true
	}
	data, err := json.Marshal(builder)
	if err != nil {
		return false
	}
	s := string(data)
	return !strings.Contains(s, `"$not":`) && !strings.Contains(s, `"$or":`)
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
