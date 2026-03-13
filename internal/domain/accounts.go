package domain

import "regexp"

const (
	accountSegmentRegex = "[a-zA-Z0-9_-]+"
	accountPattern      = "^" + accountSegmentRegex + "(:" + accountSegmentRegex + ")*$"
)

var accountRegexp = regexp.MustCompile(accountPattern)

// ValidateAccountAddress returns true if addr is a valid account address.
func ValidateAccountAddress(addr string) bool {
	return accountRegexp.MatchString(addr)
}
