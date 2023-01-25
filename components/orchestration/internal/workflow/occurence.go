package workflow

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type Occurrence struct {
	bun.BaseModel `bun:"table:workflow_occurrences,alias:u"`
	WorkflowID    string    `json:"workflowID"`
	ID            string    `json:"id" bun:"id,pk"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	Statuses      []Status  `json:"status,omitempty" bun:"rel:has-many,join:id=occurrence_id"`
}

func newOccurrence(workflowID string) Occurrence {
	now := time.Now().Round(time.Nanosecond)
	return Occurrence{
		BaseModel:  bun.BaseModel{},
		WorkflowID: workflowID,
		ID:         uuid.NewString(),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}
