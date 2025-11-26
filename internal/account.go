package ledger

import (
	"math/big"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
)

const (
	WORLD = "world"
)

type Account struct {
	bun.BaseModel `bun:"table:accounts"`

	Address          string            `json:"address" bun:"address"`
	Metadata         metadata.Metadata `json:"metadata" bun:"metadata,type:jsonb,default:'{}'"`
	FirstUsage       time.Time         `json:"firstUsage" bun:"first_usage,type:timestamp without time zone,nullzero"`
	InsertionDate    time.Time         `json:"insertionDate" bun:"insertion_date,type:timestamp without time zone,nullzero"`
	UpdatedAt        time.Time         `json:"updatedAt" bun:"updated_at,type:timestamp without time zone,nullzero"`
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
