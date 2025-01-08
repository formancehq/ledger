package controller

import (
	"fmt"
	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/ledger/internal/replication/runner"
)

// ErrModuleNotAvailable denotes an attempt to use a module not declared on a stack
type ErrModuleNotAvailable string

func (e ErrModuleNotAvailable) Error() string {
	return fmt.Sprintf("module '%s' not available", string(e))
}

func (e ErrModuleNotAvailable) Is(err error) bool {
	_, ok := err.(ErrModuleNotAvailable)
	return ok
}

func NewErrModuleNotAvailable(module string) ErrModuleNotAvailable {
	return ErrModuleNotAvailable(module)
}

// ErrConnectorNotFound denotes an attempt to use a not found connector
type ErrConnectorNotFound string

func (e ErrConnectorNotFound) Error() string {
	return fmt.Sprintf("connector '%s' not found", string(e))
}

func (e ErrConnectorNotFound) Is(err error) bool {
	_, ok := err.(ErrConnectorNotFound)
	return ok
}

func NewErrConnectorNotFound(connectorID string) ErrConnectorNotFound {
	return ErrConnectorNotFound(connectorID)
}

// ErrPipelineAlreadyExists denotes a pipeline already created
// The store is in charge of returning this error on a failing call on Store.CreatePipeline
type ErrPipelineAlreadyExists ledger.PipelineConfiguration

func (e ErrPipelineAlreadyExists) Error() string {
	return fmt.Sprintf("pipeline '%s/%s' already exists", e.Ledger, e.ConnectorID)
}

func (e ErrPipelineAlreadyExists) Is(err error) bool {
	_, ok := err.(ErrPipelineAlreadyExists)
	return ok
}

func NewErrPipelineAlreadyExists(pipelineConfiguration ledger.PipelineConfiguration) ErrPipelineAlreadyExists {
	return ErrPipelineAlreadyExists(pipelineConfiguration)
}

// ErrInUsePipeline denotes a pipeline which is actually used
// The client has to retry later if still relevant
type ErrInUsePipeline string

func (e ErrInUsePipeline) Error() string {
	return fmt.Sprintf("pipeline '%s' already in use", string(e))
}

func (e ErrInUsePipeline) Is(err error) bool {
	_, ok := err.(ErrInUsePipeline)
	return ok
}

func NewErrInUsePipeline(id string) ErrInUsePipeline {
	return ErrInUsePipeline(id)
}

type ErrInvalidDriverConfiguration struct {
	name string
	err  error
}

func (e ErrInvalidDriverConfiguration) Error() string {
	return fmt.Sprintf("driver '%s' invalid: %s", e.name, e.err)
}

func (e ErrInvalidDriverConfiguration) Is(err error) bool {
	_, ok := err.(ErrInvalidDriverConfiguration)
	return ok
}

func NewErrInvalidDriverConfiguration(name string, err error) ErrInvalidDriverConfiguration {
	return ErrInvalidDriverConfiguration{
		name: name,
		err:  err,
	}
}

type ErrConnectorUsed string

func (e ErrConnectorUsed) Error() string {
	return fmt.Sprintf("connector '%s' actually used by an existing pipeline", string(e))
}

func (e ErrConnectorUsed) Is(err error) bool {
	_, ok := err.(ErrConnectorUsed)
	return ok
}

func NewErrConnectorUsed(id string) ErrConnectorUsed {
	return ErrConnectorUsed(id)
}

// The type aliases below allow package consumers to focus on errors in this package instead of searching potential errors along the code tree

type ErrPipelineNotFound = runner.ErrPipelineNotFound
type ErrInvalidStateSwitch = runner.ErrInvalidStateSwitch
type ErrAlreadyStarted = runner.ErrAlreadyStarted
