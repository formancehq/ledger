package ledger

import (
	"fmt"
	"strings"
)

func isSegmentedAddress(address string) bool {
	src := strings.Split(address, ":")

	for _, segment := range src {
		if segment == "" {
			return true
		}
	}

	return false
}

func filterAccountAddress(address, key string) string {
	parts := make([]string, 0)

	if isPartialAddress(address) {
		src := strings.Split(address, ":")
		// Pattern semantics:
		// - "users:" = ["users", ""] = 2 segments = match "users:X"
		// - "users::" = ["users", "", ""] = 3 segments = match "users:X:Y"
		// - "users::alice" = ["users", "", "alice"] = 3 segments = match "users:X:alice"
		parts = append(parts, fmt.Sprintf("jsonb_array_length(%s_array) = %d", key, len(src)))

		for i, segment := range src {
			if len(segment) == 0 {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s_array @@ ('$[%d] == \"%s\"')::jsonpath", key, i, segment))
		}
	} else {
		parts = append(parts, fmt.Sprintf("%s = '%s'", key, address))
	}

	return strings.Join(parts, " and ")
}

func isPartialAddress(address any) bool {
	return isSegmentedAddress(address.(string))
}
