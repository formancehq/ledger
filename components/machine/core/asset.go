package core

import (
	"fmt"
	"regexp"
)

const assetPattern = "^[A-Z/0-9]+$"

var assetRegexp = regexp.MustCompile(assetPattern)

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
		return fmt.Errorf("assets should respect pattern %s", assetPattern)
	}
	return nil
}
