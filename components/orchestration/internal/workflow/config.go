package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/orchestration/internal/processor"
	"github.com/formancehq/orchestration/internal/spec"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
	"go.temporal.io/sdk/workflow"
)

type Stage map[string]map[string]any

type Config struct {
	Stages []Stage `json:"stages"`
}

func (c *Config) runStage(ctx workflow.Context, stage Stage, variables map[string]string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%s", e)
		}
	}()
	var (
		name  string
		value map[string]any
	)
	for name, value = range stage {
	}

	spec, err := spec.ResolveSpecification(name)
	if err != nil {
		return err
	}

	_, err = processor.Find(name).Run(ctx, processor.Input{
		Specification: *spec,
		Parameters:    value,
		Variables:     variables,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Config) run(ctx workflow.Context, db *bun.DB, variables map[string]string) error {
	for ind, rawStage := range c.Stages {
		status := Status{
			Stage:        ind,
			OccurrenceID: workflow.GetInfo(ctx).WorkflowExecution.RunID,
			StartedAt:    workflow.Now(ctx).Round(time.Nanosecond),
		}
		err := c.runStage(ctx, rawStage, variables)
		if err != nil {
			status.Error = err.Error()
		}
		status.TerminatedAt = workflow.Now(ctx).Round(time.Nanosecond)

		logger := logging.WithFields(map[string]any{
			"runID": workflow.GetInfo(ctx).ContinuedExecutionRunID,
		})
		if status.Error != "" {
			logger.Errorf("error running stage: %s", status.Error)
		}

		if _, dbErr := db.NewInsert().Model(&status).Exec(context.Background()); dbErr != nil {
			logger.Errorf("error inserting status into database: %s", dbErr)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) Validate() error {
	for _, rawStage := range c.Stages {
		if len(rawStage) == 0 {
			return fmt.Errorf("empty specification")
		}
		if len(rawStage) > 1 {
			return fmt.Errorf("a specification should have only one name")
		}
		var (
			name  string
			value map[string]any
		)
		for name, value = range rawStage {
		}

		spec, err := spec.ResolveSpecification(name)
		if err != nil {
			return err
		}

		if err := spec.Validate(value); err != nil {
			return errors.Wrapf(err, "validating schema for specification: %s", name)
		}
	}
	return nil
}
