package core

import (
	"fmt"
	"regexp"
)

const assetPattern = `^[A-Z][A-Z0-9]{0,16}(\/\d{1,6})?$`

var assetRegexp = regexp.MustCompile(assetPattern)

func AssetIsValid(v string) bool {
	return assetRegexp.Match([]byte(v))
}

type Asset string

func (Asset) GetType() Type { return TypeAsset }
func (a Asset) String() string {
	return fmt.Sprintf("%v", string(a))
}

type HasAsset interface {
	GetAsset() Asset
}

func (a Asset) GetAsset() Asset { return a }

func ParseAsset(ass Asset) error {
	if !assetRegexp.MatchString(string(ass)) {
		return fmt.Errorf("asset should respect pattern '%s'", assetPattern)
	}
	return nil
}
