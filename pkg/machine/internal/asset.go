package internal

import (
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
)

type Asset string

func (Asset) GetType() Type { return TypeAsset }
func (a Asset) String() string {
	return fmt.Sprintf("%v", string(a))
}

type HasAsset interface {
	GetAsset() Asset
}

func (a Asset) GetAsset() Asset { return a }

func ValidateAsset(ass Asset) error {
	if !core.AssetRegexp.MatchString(string(ass)) {
		return fmt.Errorf("asset should respect pattern '%s'", core.AssetPattern)
	}
	return nil
}
