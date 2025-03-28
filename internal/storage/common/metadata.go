package common

import "regexp"

var (
	MetadataRegex = regexp.MustCompile(`metadata\[(.+)]`)
)
