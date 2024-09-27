package machine

import (
	"fmt"
	"github.com/formancehq/ledger/pkg/accounts"
)

type AccountAddress string

func (AccountAddress) GetType() Type { return TypeAccount }
func (a AccountAddress) String() string {
	return fmt.Sprintf("@%v", string(a))
}

func ValidateAccountAddress(acc AccountAddress) error {
	if !accounts.Regexp.MatchString(string(acc)) {
		return fmt.Errorf("accounts should respect pattern %s", accounts.Pattern)
	}
	return nil
}
