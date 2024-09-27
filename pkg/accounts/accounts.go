package accounts

import "regexp"

const SegmentRegex = "[a-zA-Z0-9_-]+"
const Pattern = "^" + SegmentRegex + "(:" + SegmentRegex + ")*$"

var Regexp = regexp.MustCompile(Pattern)

func ValidateAddress(addr string) bool {
	return Regexp.Match([]byte(addr))
}
