package ledger

import (
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
)

const (
	WORLD = "world"
)

type Account struct {
	Address          string            `json:"address"`
	Metadata         metadata.Metadata `json:"metadata"`
	FirstUsage       time.Time         `json:"firstUsage"`
	InsertionDate    time.Time         `json:"insertionDate"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	Volumes          VolumesByAssets   `json:"volumes,omitempty"`
	EffectiveVolumes VolumesByAssets   `json:"effectiveVolumes,omitempty"`
}

func (a Account) GetAddress() string {
	return a.Address
}

type AccountsVolumes struct {
	Account string   `json:"account"`
	Asset   string   `json:"asset"`
	Input   *big.Int `json:"input"`
	Output  *big.Int `json:"output"`
}
