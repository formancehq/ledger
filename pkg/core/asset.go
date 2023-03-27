package core

import (
	"regexp"
)

const AssetPattern = `^[A-Z][A-Z0-9]{0,16}(\/\d{1,6})?$`

var AssetRegexp = regexp.MustCompile(AssetPattern)

func AssetIsValid(v string) bool {
	return AssetRegexp.Match([]byte(v))
}
