package ledger

import (
	"encoding/json"
	"regexp"

	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/uptrace/bun"
)

const (
	WORLD = "world"
)

type Account struct {
	bun.BaseModel `bun:"table:accounts,alias:accounts"`

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

type ExpandedAccount struct {
	Account          `bun:",extend"`
	Volumes          VolumesByAssets `json:"volumes,omitempty" bun:"volumes,type:jsonb"`
	EffectiveVolumes VolumesByAssets `json:"effectiveVolumes,omitempty" bun:"effective_volumes,type:jsonb"`
}

func NewExpandedAccount(address string) ExpandedAccount {
	return ExpandedAccount{
		Account: Account{
			Address:  address,
			Metadata: metadata.Metadata{},
		},
		Volumes: map[string]*Volumes{},
	}
}

func (v ExpandedAccount) MarshalJSON() ([]byte, error) {
	type aux ExpandedAccount
	return json.Marshal(struct {
		aux
		Balances BalancesByAssets `json:"balances"`
	}{
		aux:      aux(v),
		Balances: v.Volumes.Balances(),
	})
}

func (v ExpandedAccount) Copy() ExpandedAccount {
	v.Account = v.Account.copy()
	v.Volumes = v.Volumes.copy()
	return v
}

const AccountPattern = "^[a-zA-Z_]+[a-zA-Z0-9_:]*$"

var AccountRegexp = regexp.MustCompile(AccountPattern)
