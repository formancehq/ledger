package ledger

import (
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/uptrace/bun"
	"math/big"
)

const (
	WORLD = "world"
)

type Account struct {
	bun.BaseModel `bun:"table:accounts"`

	Address          string            `json:"address" bun:"address"`
	Metadata         metadata.Metadata `json:"metadata" bun:"metadata,type:jsonb,default:'{}'"`
	FirstUsage       time.Time         `json:"-" bun:"first_usage,nullzero"`
	InsertionDate    time.Time         `json:"-" bun:"insertion_date,nullzero"`
	UpdatedAt        time.Time         `json:"-" bun:"updated_at,nullzero"`
	Volumes          VolumesByAssets   `json:"volumes,omitempty" bun:"volumes,scanonly"`
	EffectiveVolumes VolumesByAssets   `json:"effectiveVolumes,omitempty" bun:"effective_volumes,scanonly"`
}

func (a Account) GetAddress() string {
	return a.Address
}

type AccountsVolumes struct {
	bun.BaseModel `bun:"accounts_volumes"`

	Account string   `bun:"accounts_address,type:varchar"`
	Asset   string   `bun:"asset,type:varchar"`
	Input   *big.Int `bun:"input,type:numeric"`
	Output  *big.Int `bun:"output,type:numeric"`
}
