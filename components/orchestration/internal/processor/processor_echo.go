package processor

import (
	"github.com/formancehq/orchestration/internal/crawler"
	"github.com/formancehq/orchestration/internal/spec"
	"go.temporal.io/sdk/workflow"
)

func Echo(ctx workflow.Context, input Input) (any, error) {
	return crawler.New(
		input.Specification.ObjectSchema,
		input.Parameters,
		crawler.NewContext().WithVariables(input.Variables),
	).GetProperty("message").AsString(), nil
}

func init() {
	RegisterProcessor(spec.EchoLabel, Func(Echo))
}
