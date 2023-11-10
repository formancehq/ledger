package machine

import (
	"fmt"

	ledger "github.com/formancehq/ledger/internal"
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
	if !ledger.AssetRegexp.MatchString(string(ass)) {
		return fmt.Errorf("asset should respect pattern '%s'", ledger.AssetPattern)
	}
	return nil
}
