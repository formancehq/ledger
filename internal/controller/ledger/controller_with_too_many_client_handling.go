package ledger

import (
	"context"
	"database/sql"
	"errors"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"time"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller_with_too_many_client_handling.go -destination controller_with_too_many_client_handling_generated_test.go -package ledger . DelayCalculator -typed
type DelayCalculator interface {
	Next(int) time.Duration
}
type DelayCalculatorFn func(int) time.Duration

func (fn DelayCalculatorFn) Next(iteration int) time.Duration {
	return fn(iteration)
}

type ControllerWithTooManyClientHandling struct {
	Controller
	delayCalculator DelayCalculator
	tracer          trace.Tracer
}

func NewControllerWithTooManyClientHandling(
	underlying Controller,
	tracer trace.Tracer,
	delayCalculator DelayCalculator,
) *ControllerWithTooManyClientHandling {
	return &ControllerWithTooManyClientHandling{
		Controller:      underlying,
		delayCalculator: delayCalculator,
		tracer:          tracer,
	}
}

func (c *ControllerWithTooManyClientHandling) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	var (
		log                *ledger.Log
		createdTransaction *ledger.CreatedTransaction
		err                error
	)
	err = handleRetry(ctx, c.tracer, c.delayCalculator, func(ctx context.Context) error {
		log, createdTransaction, err = c.Controller.CreateTransaction(ctx, parameters)
		return err
	})
	return log, createdTransaction, err
}

func (c *ControllerWithTooManyClientHandling) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	var (
		log                 *ledger.Log
		revertedTransaction *ledger.RevertedTransaction
		err                 error
	)
	err = handleRetry(ctx, c.tracer, c.delayCalculator, func(ctx context.Context) error {
		log, revertedTransaction, err = c.Controller.RevertTransaction(ctx, parameters)
		return err

	})
	return log, revertedTransaction, err
}

func (c *ControllerWithTooManyClientHandling) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = handleRetry(ctx, c.tracer, c.delayCalculator, func(ctx context.Context) error {
		log, err = c.Controller.SaveTransactionMetadata(ctx, parameters)
		return err
	})

	return log, err
}

func (c *ControllerWithTooManyClientHandling) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = handleRetry(ctx, c.tracer, c.delayCalculator, func(ctx context.Context) error {
		log, err = c.Controller.SaveAccountMetadata(ctx, parameters)
		return err
	})

	return log, err
}

func (c *ControllerWithTooManyClientHandling) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = handleRetry(ctx, c.tracer, c.delayCalculator, func(ctx context.Context) error {
		log, err = c.Controller.DeleteTransactionMetadata(ctx, parameters)
		return err
	})

	return log, err
}

func (c *ControllerWithTooManyClientHandling) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = handleRetry(ctx, c.tracer, c.delayCalculator, func(ctx context.Context) error {
		log, err = c.Controller.DeleteAccountMetadata(ctx, parameters)
		return err
	})

	return log, err
}

func (c *ControllerWithTooManyClientHandling) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, error) {
	ctrl, err := c.Controller.BeginTX(ctx, options)
	if err != nil {
		return nil, err
	}

	return &ControllerWithTooManyClientHandling{
		Controller:      ctrl,
		delayCalculator: c.delayCalculator,
		tracer:          c.tracer,
	}, nil
}

var _ Controller = (*ControllerWithTooManyClientHandling)(nil)

func handleRetry(
	ctx context.Context,
	tracer trace.Tracer,
	delayCalculator DelayCalculator,
	fn func(ctx context.Context) error,
) error {

	ctx, span := tracer.Start(ctx, "TooManyClientRetrier")
	defer span.End()

	count := 0
	for {
		err := fn(ctx)
		if err != nil && errors.Is(err, postgres.ErrTooManyClient{}) {
			delay := delayCalculator.Next(count)
			if delay == 0 {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				count++
				span.SetAttributes(attribute.Int("retry", count))
				continue
			}
		}
		return err
	}
}
