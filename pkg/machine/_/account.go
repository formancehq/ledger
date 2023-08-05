package internal

import (
	"fmt"

	"github.com/numary/ledger/pkg/core"
)

type AccountAddress string

func (AccountAddress) GetType() Type { return TypeAccount }
func (a AccountAddress) String() string {
	return fmt.Sprintf("@%v", string(a))
}

func ValidateAccountAddress(acc AccountAddress) error {
	if !core.AccountRegexp.MatchString(string(acc)) {
		return fmt.Errorf("accounts should respect pattern %s", core.AccountPattern)
	}
	return nil
}
