package sqlstorage

import (
	"regexp"
	"strings"
)

func formatMetadataKey(key string) string {
	metadataKeyRegexp := regexp.MustCompile(`(".+"|[^.])+`)
	matches := metadataKeyRegexp.FindAllString(key, -1)

	return strings.ReplaceAll(
		strings.Join(matches, "', '"), "\"", "",
	)
}
