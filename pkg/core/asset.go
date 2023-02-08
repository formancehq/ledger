package core

import (
	"regexp"
)

var assetRegexp = regexp.MustCompile(`^[A-Z][A-Z0-9]{0,16}(\/\d{1,6})?$`)

func AssetIsValid(v string) bool {
	return assetRegexp.Match([]byte(v))
}
