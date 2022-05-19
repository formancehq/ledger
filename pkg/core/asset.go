package core

import (
	"regexp"
)

var assetRegexp = regexp.MustCompile(`^[A-Z]{1,16}(\/\d{1,6})?$`)

func AssetIsValid(v string) bool {
	return assetRegexp.Match([]byte(v))
}
