package domain

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const assetPattern = `^[A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?(\/\d{1,6})?$`

var assetRegexp = regexp.MustCompile(assetPattern)

// ValidateAsset returns true if v is a valid asset identifier.
func ValidateAsset(v string) bool {
	return assetRegexp.MatchString(v)
}

// ParseAssetPrecision splits an asset string into its base name and precision.
// "USD/4" → ("USD", 4), "EUR" → ("EUR", 0).
func ParseAssetPrecision(asset string) (string, uint8) {
	base, precStr, found := strings.Cut(asset, "/")
	if !found {
		return asset, 0
	}

	prec, _ := strconv.ParseUint(precStr, 10, 8)

	return base, uint8(prec)
}

// FormatAsset reconstructs an asset string from base and precision.
// ("USD", 4) → "USD/4", ("EUR", 0) → "EUR".
func FormatAsset(base string, precision uint8) string {
	if precision == 0 {
		return base
	}

	return fmt.Sprintf("%s/%d", base, precision)
}
