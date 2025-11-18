package ledger

import (
	"fmt"
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

func explodeAddress(address string) map[string]any {
	parts := strings.Split(address, ":")
	ret := make(map[string]any, len(parts)+1)
	for i, part := range parts {
		ret[fmt.Sprint(i)] = part
	}
	ret[fmt.Sprint(len(parts))] = nil

	return ret
}

func isFilteringOnPartialAddress(value any) bool {
	switch value := value.(type) {
	case string:
		return isPartialAddress(value)
	default:
		// If an array is passed, addresses must be full
		return false
	}
}
