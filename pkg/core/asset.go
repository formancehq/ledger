package core

import "regexp"

func AssetIsValid(v string) bool {
	re := regexp.MustCompile("[A-Z]{1,8}")

	return re.Match([]byte(v))
}
