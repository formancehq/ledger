package domain

import "regexp"

const assetPattern = `^[A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?(\/\d{1,6})?$`

var assetRegexp = regexp.MustCompile(assetPattern)

// ValidateAsset returns true if v is a valid asset identifier.
func ValidateAsset(v string) bool {
	return assetRegexp.MatchString(v)
}
