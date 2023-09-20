package internal

import (
	"fmt"

	ledger "github.com/formancehq/ledger/internal"
)

type AccountAddress string

func (AccountAddress) GetType() Type { return TypeAccount }
func (a AccountAddress) String() string {
	return fmt.Sprintf("@%v", string(a))
}

func ValidateAccountAddress(acc AccountAddress) error {
	if !ledger.AccountRegexp.MatchString(string(acc)) {
		return fmt.Errorf("accounts should respect pattern %s", ledger.AccountPattern)
	}
	return nil
}
