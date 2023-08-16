package core

import (
	"encoding/json"
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

func (v Account) Copy() *Account {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	ret := &Account{}
	if err := json.Unmarshal(data, ret); err != nil {
		panic(err)
	}
	return ret
}

type AccountWithVolumes struct {
	Account
	Volumes  AssetsVolumes  `json:"volumes"`
	Balances AssetsBalances `json:"balances" example:"COIN:100"`
}

func (v AccountWithVolumes) Copy() *AccountWithVolumes {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	ret := &AccountWithVolumes{}
	if err := json.Unmarshal(data, ret); err != nil {
		panic(err)
	}
	return ret
}

const accountPattern = "^[a-zA-Z_]+[a-zA-Z0-9_:]*$"

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
