package ledger

import (
	"fmt"
	"strings"
)

func isSegmentedAddress(address string) bool {
	src := strings.Split(address, ":")

	needSegmentCheck := false
	for _, segment := range src {
		needSegmentCheck = segment == ""
		if needSegmentCheck {
			break
		}
	}

	return needSegmentCheck
}

func filterAccountAddress(address, key string) string {
	parts := make([]string, 0)

	if isPartialAddress(address) {
		src := strings.Split(address, ":")
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

func isPartialAddress[V any](address V) bool {
	switch address := any(address).(type) {
	case string:
		for _, segment := range strings.Split(address, ":") {
			if segment == "" {
				return true
			}
		}
	}

	return false
}
