package ledger

import (
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

func (v ExpandedAccount) Copy() ExpandedAccount {
	v.Account = v.Account.copy()
	v.Volumes = v.Volumes.copy()
	return v
}

const AccountSegmentRegex = "[a-zA-Z0-9_]+(?:-[a-zA-Z0-9_]+)*"
const AccountPattern = "^" + AccountSegmentRegex + "(:" + AccountSegmentRegex + ")*$"

var AccountRegexp = regexp.MustCompile(AccountPattern)

func ValidateAddress(addr string) bool {
	return AccountRegexp.Match([]byte(addr))
}
