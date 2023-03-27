package core

import (
	"encoding/json"
	"regexp"
)

const (
	WORLD = "world"
)

type Account struct {
	Address  string   `json:"address" example:"users:001"`
	Metadata Metadata `json:"metadata" swaggertype:"object"`
}

func (a Account) copy() Account {
	a.Metadata = a.Metadata.copy()
	return a
}

func NewAccount(address string) Account {
	return Account{
		Address:  address,
		Metadata: Metadata{},
	}
}

type AccountWithVolumes struct {
	Account
	Volumes AssetsVolumes `json:"volumes"`
}

func (v AccountWithVolumes) MarshalJSON() ([]byte, error) {
	type aux AccountWithVolumes
	return json.Marshal(struct {
		aux
		Balances AssetsBalances `json:"balances"`
	}{
		aux:      aux(v),
		Balances: v.Volumes.Balances(),
	})
}

func (v AccountWithVolumes) Copy() AccountWithVolumes {
	v.Account = v.Account.copy()
	v.Volumes = v.Volumes.copy()
	return v
}

const AccountPattern = "^[a-zA-Z_]+[a-zA-Z0-9_:]*$"

var AccountRegexp = regexp.MustCompile(AccountPattern)
