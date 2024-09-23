package assets

import (
	"regexp"
)

const Pattern = `[A-Z][A-Z0-9]{0,16}(\/\d{1,6})?`

var Regexp = regexp.MustCompile("^" + Pattern + "$")

func IsValid(v string) bool {
	return Regexp.Match([]byte(v))
}
