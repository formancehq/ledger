package workflow

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type Workflow struct {
	bun.BaseModel `bun:"table:workflows"`
	ID            string    `json:"id" bun:",pk"`
	Config        Config    `json:"config"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

func New(config Config) Workflow {
	now := time.Now().Round(time.Nanosecond)
	return Workflow{
		ID:        uuid.NewString(),
		Config:    config,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
