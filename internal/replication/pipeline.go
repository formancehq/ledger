package ingester

import (
	"fmt"

	"github.com/formancehq/go-libs/v2/time"
	"github.com/google/uuid"
)

type PipelineConfiguration struct {
	Ledger      string `json:"ledger"`
	ConnectorID string `json:"connectorID"`
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
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
	State     State     `json:"state"`
	PipelineConfiguration
}

func (p Pipeline) String() string {
	return fmt.Sprintf("%s (%s): %s", p.ID, p.PipelineConfiguration, p.State)
}

func NewPipeline(pipelineConfiguration PipelineConfiguration, state State) Pipeline {
	return Pipeline{
		ID:                    uuid.NewString(),
		PipelineConfiguration: pipelineConfiguration,
		State:                 state,
		CreatedAt:             time.Now(),
	}
}