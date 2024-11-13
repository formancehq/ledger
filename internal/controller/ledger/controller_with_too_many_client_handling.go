package ledger

import (
	"context"
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

func (ctrl *ControllerWithTooManyClientHandling) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	return handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, ctrl.Controller.CreateTransaction)
}

func (ctrl *ControllerWithTooManyClientHandling) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, ctrl.Controller.RevertTransaction)
}

func (ctrl *ControllerWithTooManyClientHandling) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	log, _, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, *struct{}, error) {
		log, err := ctrl.Controller.SaveTransactionMetadata(ctx, parameters)
		return log, nil, err
	})

	return log, err
}

func (ctrl *ControllerWithTooManyClientHandling) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	log, _, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, *struct{}, error) {
		log, err := ctrl.Controller.SaveAccountMetadata(ctx, parameters)
		return log, nil, err
	})
	return log, err
}

func (ctrl *ControllerWithTooManyClientHandling) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	log, _, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, *struct{}, error) {
		log, err := ctrl.Controller.DeleteTransactionMetadata(ctx, parameters)
		return log, nil, err
	})
	return log, err
}

func (ctrl *ControllerWithTooManyClientHandling) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	log, _, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, *struct{}, error) {
		log, err := ctrl.Controller.DeleteAccountMetadata(ctx, parameters)
		return log, nil, err
	})
	return log, err
}

var _ Controller = (*ControllerWithTooManyClientHandling)(nil)

func handleRetry[INPUT, OUTPUT any](
	ctx context.Context,
	tracer trace.Tracer,
	delayCalculator DelayCalculator,
	parameters Parameters[INPUT],
	fn func(ctx context.Context, parameters Parameters[INPUT]) (*ledger.Log, *OUTPUT, error),
) (*ledger.Log, *OUTPUT, error) {

	ctx, span := tracer.Start(ctx, "TooManyClientRetrier")
	defer span.End()

	count := 0
	for {
		log, output, err := fn(ctx, parameters)
		if err != nil && errors.Is(err, postgres.ErrTooManyClient{}) {
			delay := delayCalculator.Next(count)
			if delay == 0 {
				return nil, nil, err
			}
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(delay):
				count++
				span.SetAttributes(attribute.Int("retry", count))
				continue
			}
		}
		return log, output, err
	}
}
