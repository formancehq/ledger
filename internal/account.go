package ledger

import (
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/time"
	"github.com/uptrace/bun"
)

const (
	WORLD = "world"
)

type Account struct {
	bun.BaseModel `bun:"table:accounts"`

	Address          string            `json:"address" bun:"address"`
	Metadata         metadata.Metadata `json:"metadata" bun:"metadata,type:jsonb"`
	FirstUsage       time.Time         `json:"-" bun:"first_usage"`
	InsertionDate    time.Time         `json:"_" bun:"insertion_date"`
	UpdatedAt        time.Time         `json:"-" bun:"updated_at"`
	Volumes          VolumesByAssets   `json:"volumes,omitempty" bun:"pcv,scanonly"`
	EffectiveVolumes VolumesByAssets   `json:"effectiveVolumes,omitempty" bun:"pcev,scanonly"`
}