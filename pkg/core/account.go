package core

import (
	"encoding/json"
	"regexp"

	"github.com/formancehq/stack/libs/go-libs/metadata"
)

const (
	WORLD = "world"
)

type Account struct {
	Address  string            `json:"address"`
	Metadata metadata.Metadata `json:"metadata"`
}

func (a Account) copy() Account {
	a.Metadata = a.Metadata.Copy()
	return a
}

func NewAccount(address string) Account {
	return Account{
		Address:  address,
		Metadata: metadata.Metadata{},
	}
}

type AccountWithVolumes struct {
	Account
	Volumes VolumesByAssets `json:"volumes"`
}

func NewAccountWithVolumes(address string) *AccountWithVolumes {
	return &AccountWithVolumes{
		Account: Account{
			Address:  address,
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]*Volumes{},
	}
}

func (v AccountWithVolumes) MarshalJSON() ([]byte, error) {
	type aux AccountWithVolumes
	return json.Marshal(struct {
		aux
		Balances BalancesByAssets `json:"balances"`
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
