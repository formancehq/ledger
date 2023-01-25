package workflow

import (
	"time"

	"github.com/uptrace/bun"
)

type Status struct {
	bun.BaseModel `bun:"table:workflow_stage_statuses"`
	Stage         int       `json:"stage" bun:"stage,pk"`
	OccurrenceID  string    `json:"occurrenceID" bun:"occurrence_id,pk"`
	StartedAt     time.Time `json:"startedAt"`
	TerminatedAt  time.Time `json:"terminatedAt"`
	Error         string    `json:"error"`
}
