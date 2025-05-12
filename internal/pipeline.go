package ledger

import (
	"fmt"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/google/uuid"
)

type PipelineConfiguration struct {
	Ledger      string `json:"ledger" bun:"ledger"`
	ConnectorID string `json:"connectorID" bun:"connector_id"`
}

func (p PipelineConfiguration) String() string {
	return fmt.Sprintf("%s/%s", p.Ledger, p.ConnectorID)
}

func NewPipelineConfiguration(ledger, connectorID string) PipelineConfiguration {
	return PipelineConfiguration{
		Ledger:      ledger,
		ConnectorID: connectorID,
	}
}

type Pipeline struct {
	bun.BaseModel `bun:"table:_system.pipelines"`

	PipelineConfiguration
	CreatedAt time.Time `json:"createdAt" bun:"created_at"`
	ID        string    `json:"id" bun:"id,pk"`
	Enabled   bool      `json:"enabled" bun:"enabled"`
	LastLogID *uint64   `json:"lastLogID,omitempty" bun:"last_log_id"`
	Error     string    `json:"error,omitempty" bun:"error"`
}

func NewPipeline(pipelineConfiguration PipelineConfiguration) Pipeline {
	return Pipeline{
		ID:                    uuid.NewString(),
		PipelineConfiguration: pipelineConfiguration,
		Enabled:               true,
		CreatedAt:             time.Now(),
		LastLogID:             nil,
	}
}
