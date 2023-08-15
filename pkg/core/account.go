package core

import (
	"fmt"
	"regexp"
)

const (
	WORLD = "world"
)

type Account struct {
	Address  AccountAddress `json:"address" example:"users:001"`
	Metadata Metadata       `json:"metadata" swaggertype:"object"`
}

type AccountWithVolumes struct {
	Account
	Volumes  AssetsVolumes  `json:"volumes"`
	Balances AssetsBalances `json:"balances" example:"COIN:100"`
}

const accountPattern = "^[a-zA-Z_0-9]+[a-zA-Z0-9_:]*$"

var accountRegexp = regexp.MustCompile(accountPattern)

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
	if !accountRegexp.MatchString(string(acc)) {
		return fmt.Errorf("accounts should respect pattern %s", accountPattern)
	}
	return nil
}
