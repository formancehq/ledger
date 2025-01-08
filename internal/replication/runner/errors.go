package runner

import (
	"fmt"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/ledger/internal/replication/drivers"
)

type ErrPipelineNotFound string

func (e ErrPipelineNotFound) Error() string {
	return fmt.Sprintf("pipeline '%s' not found", string(e))
}

func (e ErrPipelineNotFound) Is(err error) bool {
	_, ok := err.(ErrPipelineNotFound)
	return ok
}

func NewErrPipelineNotFound(id string) ErrPipelineNotFound {
	return ErrPipelineNotFound(id)
}

type ErrInvalidStateSwitch struct {
	id        string
	fromState ledger.PipelineStateLabel
	toState   ledger.PipelineStateLabel
}

func (e ErrInvalidStateSwitch) Error() string {
	return fmt.Sprintf(
		"unable to switch from state '%s' to state '%s' for pipeline '%s'",
		e.fromState,
		e.toState,
		e.id,
	)
}

func (e ErrInvalidStateSwitch) Is(err error) bool {
	_, ok := err.(ErrInvalidStateSwitch)
	return ok
}

func NewErrInvalidStateSwitch(id string, fromState, toState ledger.PipelineStateLabel) ErrInvalidStateSwitch {
	return ErrInvalidStateSwitch{
		id:        id,
		fromState: fromState,
		toState:   toState,
	}
}

type ErrAlreadyStarted string

func (e ErrAlreadyStarted) Error() string {
	return fmt.Sprintf("pipeline '%s' already started", string(e))
}

func (e ErrAlreadyStarted) Is(err error) bool {
	_, ok := err.(ErrAlreadyStarted)
	return ok
}

func NewErrAlreadyStarted(id string) ErrAlreadyStarted {
	return ErrAlreadyStarted(id)
}

type ErrConnectorNotFound = drivers.ErrConnectorNotFound
