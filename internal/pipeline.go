package ledger

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v4/time"
)

type PipelineConfiguration struct {
	Ledger     string `json:"ledger" bun:"ledger"`
	ExporterID string `json:"exporterID" bun:"exporter_id"`
}

func (p PipelineConfiguration) String() string {
	return fmt.Sprintf("%s/%s", p.Ledger, p.ExporterID)
}

func NewPipelineConfiguration(ledger, exporterID string) PipelineConfiguration {
	return PipelineConfiguration{
		Ledger:     ledger,
		ExporterID: exporterID,
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
