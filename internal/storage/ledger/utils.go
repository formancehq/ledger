package ledger

import (
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v3/query"
	"strings"
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

// queryHasNegation detects if a query builder contains any $not clauses
// by inspecting its JSON representation. When $not is present, pushing a
// positive address filter into the LATERAL join would be incorrect (the
// lateral keeps only matching rows, but $not excludes them).
func queryHasNegation(builder query.Builder) bool {
	if builder == nil {
		return false
	}
	data, err := json.Marshal(builder)
	if err != nil {
		return false
	}
	return strings.Contains(string(data), `"$not":`)
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
