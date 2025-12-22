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
		// Fix trailing empty segment handling for patterns ending with "::"
		// strings.Split("users::", ":") gives ["users", "", ""] (3 elements)
		// but "users::" should match addresses with 2 segments like "users:alice"
		//
		// The rule: if the address ends with "::" (double colon), the last empty
		// element is an artifact of Split and should be removed.
		// But only if there's nothing but empty segments after the last non-empty one.
		//
		// Examples:
		// - "users::" -> ["users", "", ""] -> expectedLen = 2 (match users:X)
		// - "users::products::" -> ["users", "", "products", "", ""] -> expectedLen = 5 (match users:X:products:Y:Z)
		// - "users:" -> ["users", ""] -> expectedLen = 2 (match users:X)
		expectedLen := len(src)
		// Only trim if the pattern ends with "::" AND the last two are empty AND
		// there's no non-empty segment between them
		if expectedLen >= 2 && src[expectedLen-1] == "" && src[expectedLen-2] == "" {
			// Check if this is a "simple ending" pattern like "users::" vs "users::products::"
			// For "users::", we want to trim. For "users::products::", we don't.
			// The difference: in "users::", there are only empty segments after the first non-empty
			hasNonEmptyAfterFirst := false
			for i := 1; i < expectedLen-1; i++ {
				if src[i] != "" {
					hasNonEmptyAfterFirst = true
					break
				}
			}
			if !hasNonEmptyAfterFirst {
				expectedLen--
			}
		}
		parts = append(parts, fmt.Sprintf("jsonb_array_length(%s_array) = %d", key, expectedLen))

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
