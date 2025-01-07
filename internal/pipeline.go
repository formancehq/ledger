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
	CreatedAt time.Time `json:"createdAt"`
	ID    string `json:"id"`
	State State  `json:"state"`
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

type StateLabel string

const (
	StateLabelInit  StateLabel = "INIT"
	StateLabelReady StateLabel = "READY"
	StateLabelPause StateLabel = "PAUSE"
	StateLabelStop  StateLabel = "STOP"
)

type State struct {
	Label StateLabel `json:"label"`
	// Cursor can be set only if Label == StateLabelInit
	LastID uint `json:"lastID,omitempty"`
	// PreviousState can be set only if Label == StateLabelPause or Label == StateLabelStop
	PreviousState *State `json:"previousState,omitempty"`
	Error         string `json:"error,omitempty"`
}

func (s State) String() string {
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

func NewReadyStateWithID(lastID uint) State {
	return State{
		Label:  StateLabelReady,
		LastID: lastID,
	}
}

func NewStopState(previousState State) State {
	return State{
		Label:         StateLabelStop,
		PreviousState: &previousState,
	}
}

func NewReadyState() State {
	return State{
		Label:  StateLabelReady,
		LastID: 0,
	}
}

func NewPauseState(previousState State) State {
	return State{
		Label:         StateLabelPause,
		PreviousState: &previousState,
	}
}

func NewInitState() State {
	return State{
		Label: StateLabelInit,
	}
}
