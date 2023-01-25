package workflow

import (
	"github.com/formancehq/go-libs/logging"
	"github.com/uptrace/bun"
	"go.temporal.io/sdk/workflow"
)

type Runner struct {
	db *bun.DB
}

func (r Runner) Run(ctx workflow.Context, input Input) error {
	err := input.Config.run(ctx, r.db, input.Variables)
	if err != nil {
		logging.Errorf("error running workflow: %s", err)
	}
	return err
}

var Run = Runner{}.Run

func NewRunner(db *bun.DB) Runner {
	return Runner{
		db: db,
	}
}
