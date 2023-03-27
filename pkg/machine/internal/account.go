package internal

import (
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
)

type AccountAddress string

func (AccountAddress) GetType() Type { return TypeAccount }
func (a AccountAddress) String() string {
	return fmt.Sprintf("@%v", string(a))
}

func ParseAccountAddress(acc AccountAddress) error {
	// TODO: handle properly in ledger v1.10
	if acc == "" {
		return nil
	}
	if !core.AccountRegexp.MatchString(string(acc)) {
		return fmt.Errorf("accounts should respect pattern %s", core.AccountPattern)
	}
	return nil
}
