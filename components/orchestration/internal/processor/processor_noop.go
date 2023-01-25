package processor

import (
	"github.com/formancehq/orchestration/internal/spec"
	"go.temporal.io/sdk/workflow"
)

func Noop(ctx workflow.Context, input Input) (any, error) {
	return nil, nil
}

func init() {
	RegisterProcessor(spec.NoopSpecificationLabel, Func(Noop))
}
