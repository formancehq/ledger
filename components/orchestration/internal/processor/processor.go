package processor

import (
	"go.temporal.io/sdk/workflow"
)

type Processor interface {
	Run(ctx workflow.Context, input Input) (any, error)
}
type Func func(ctx workflow.Context, input Input) (any, error)

func (fn Func) Run(ctx workflow.Context, input Input) (any, error) {
	return fn(ctx, input)
}

var processors = map[string]Processor{}

func RegisterProcessor(name string, p Processor) {
	processors[name] = p
}

func Find(name string) Processor {
	return processors[name]
}
