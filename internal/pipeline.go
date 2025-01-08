package ledger

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
	CreatedAt time.Time     `json:"createdAt"`
	ID        string        `json:"id"`
	State     PipelineState `json:"state"`
	PipelineConfiguration
}

func (p Pipeline) String() string {
	return fmt.Sprintf("%s (%s): %s", p.ID, p.PipelineConfiguration, p.State)
}

func NewPipeline(pipelineConfiguration PipelineConfiguration, state PipelineState) Pipeline {
	return Pipeline{
		ID:                    uuid.NewString(),
		PipelineConfiguration: pipelineConfiguration,
		State:                 state,
		CreatedAt:             time.Now(),
	}
}

type PipelineStateLabel string

const (
	StateLabelInit  PipelineStateLabel = "INIT"
	StateLabelReady PipelineStateLabel = "READY"
	StateLabelPause PipelineStateLabel = "PAUSE"
	StateLabelStop  PipelineStateLabel = "STOP"
)

type PipelineState struct {
	Label PipelineStateLabel `json:"label"`
	// Cursor can be set only if Label == StateLabelInit
	LastID int `json:"lastID,omitempty"`
	// PreviousState can be set only if Label == StateLabelPause or Label == StateLabelStop
	PreviousState *PipelineState `json:"previousState,omitempty"`
	Error         string         `json:"error,omitempty"`
}

func (s PipelineState) String() string {
	switch s.Label {
	case StateLabelInit:
		return "INIT"
	case StateLabelReady:
		return "READY"
	case StateLabelPause:
		return "PAUSE"
	case StateLabelStop:
		return "STOP"
	default:
		return "UNKNOWN_STATE"
	}
}

func NewReadyStateWithID(lastID int) PipelineState {
	return PipelineState{
		Label:  StateLabelReady,
		LastID: lastID,
	}
}

func NewStopState(previousState PipelineState) PipelineState {
	return PipelineState{
		Label:         StateLabelStop,
		PreviousState: &previousState,
	}
}

func NewReadyState() PipelineState {
	return PipelineState{
		Label:  StateLabelReady,
		LastID: 0,
	}
}

func NewPauseState(previousState PipelineState) PipelineState {
	return PipelineState{
		Label:         StateLabelPause,
		PreviousState: &previousState,
	}
}

func NewInitState() PipelineState {
	return PipelineState{
		Label: StateLabelInit,
	}
}
