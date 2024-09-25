package machine

import (
	"fmt"

	"github.com/formancehq/ledger/v2/pkg/core/assets"
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
	if !assets.Regexp.MatchString(string(ass)) {
		return fmt.Errorf("asset should respect pattern '%s'", assets.Pattern)
	}
	return nil
}
