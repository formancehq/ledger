package ledger

import (
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/time"
)

const (
	WORLD = "world"
)

type Account struct {
	Address          string            `json:"address"`
	Metadata         metadata.Metadata `json:"metadata"`
	FirstUsage       time.Time         `json:"-"`
	InsertionDate    time.Time         `json:"_"`
	UpdatedAt        time.Time         `json:"-"`
	Volumes          VolumesByAssets   `json:"volumes,omitempty"`
	EffectiveVolumes VolumesByAssets   `json:"effectiveVolumes,omitempty"`
}
