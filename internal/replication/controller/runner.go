package controller

import (
	"context"

	ingester "github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/runner"
)

//go:generate mockgen -source runner.go -destination runner_generated.go -package controller . Pipeline
type Pipeline interface {
	Pause() error
	Resume() error
	Reset() error
	Stop() error
	GetActiveState() *runner.Signal[ingester.State]
}

//go:generate mockgen -source runner.go -destination runner_generated.go -package controller . Runner
type Runner interface {
	GetPipeline(id string) (Pipeline, bool)
	StartPipeline(ctx context.Context, pipeline ingester.Pipeline) (Pipeline, error)
}

type defaultRunner struct {
	runner *runner.Runner
}

func (d *defaultRunner) GetPipeline(id string) (Pipeline, bool) {
	ret := d.runner.GetPipeline(id)
	return ret, ret != nil
}

func (d *defaultRunner) StartPipeline(ctx context.Context, pipeline ingester.Pipeline) (Pipeline, error) {
	return d.runner.StartPipeline(ctx, pipeline)
}

func NewDefaultRunner(runner *runner.Runner) Runner {
	return &defaultRunner{
		runner: runner,
	}
}

var _ Runner = (*defaultRunner)(nil)
