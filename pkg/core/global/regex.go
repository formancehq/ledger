package global

import (
	"regexp"
)

var (
	// RegexSourceOrDestinationFormat
	RegexSourceOrDestinationFormat = regexp.MustCompile("^[a-zA-Z_0-9]+(:[a-zA-Z_0-9]+){0,}$")
	// RegexAssetFormat
	RegexAssetFormat = regexp.MustCompile("^[A-Z]{1,16}$")
)
