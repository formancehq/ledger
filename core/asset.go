package core

import (
	"regexp"
)

// AssetIsValid
func AssetIsValid(asset string) bool {
	valid, _ := regexp.MatchString("^[A-Z]{1,16}$", asset)
	return valid
}
