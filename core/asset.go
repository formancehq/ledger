package core

import (
	"log"
	"regexp"
)

// AssetIsValid
func AssetIsValid(asset string) bool {
	valid, err := regexp.MatchString("^[A-Z]{1,16}$", asset)
	if err != nil {
		log.Panic("AssetIsValid: regex invalid")
	}
	return valid
}
