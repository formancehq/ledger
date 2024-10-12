package ledger

import (
	"context"
	"errors"
	"github.com/formancehq/go-libs/platform/postgres"
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
	tracer trace.Tracer
}

func NewControllerWithTooManyClientHandling(
	underlying Controller,
	tracer trace.Tracer,
	delayCalculator DelayCalculator,
) *ControllerWithTooManyClientHandling {
	return &ControllerWithTooManyClientHandling{
		Controller:      underlying,
		delayCalculator: delayCalculator,
		tracer: tracer,
	}
}

func (ctrl *ControllerWithTooManyClientHandling) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.CreatedTransaction, error) {
	return handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, ctrl.Controller.CreateTransaction)
}

func (ctrl *ControllerWithTooManyClientHandling) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.RevertedTransaction, error) {
	return handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, ctrl.Controller.RevertTransaction)
}

func (ctrl *ControllerWithTooManyClientHandling) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) error {
	_, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*struct{}, error) {
		return nil, ctrl.Controller.SaveTransactionMetadata(ctx, parameters)
	})
	return err
}

func (ctrl *ControllerWithTooManyClientHandling) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) error {
	_, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*struct{}, error) {
		return nil, ctrl.Controller.SaveAccountMetadata(ctx, parameters)
	})
	return err
}

func (ctrl *ControllerWithTooManyClientHandling) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) error {
	_, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*struct{}, error) {
		return nil, ctrl.Controller.DeleteTransactionMetadata(ctx, parameters)
	})
	return err
}

func (ctrl *ControllerWithTooManyClientHandling) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) error {
	_, err := handleRetry(ctx, ctrl.tracer, ctrl.delayCalculator, parameters, func(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*struct{}, error) {
		return nil, ctrl.Controller.DeleteAccountMetadata(ctx, parameters)
	})
	return err
}

var _ Controller = (*ControllerWithTooManyClientHandling)(nil)

func handleRetry[INPUT, OUTPUT any](
	ctx context.Context,
	tracer trace.Tracer,
	delayCalculator DelayCalculator,
	parameters Parameters[INPUT],
	fn func(ctx context.Context, parameters Parameters[INPUT]) (*OUTPUT, error),
) (*OUTPUT, error) {

	ctx, span := tracer.Start(ctx, "Retrier")
	defer span.End()

	count := 0
	for {
		output, err := fn(ctx, parameters)
		if err != nil && errors.Is(err, postgres.ErrTooManyClient{}) {
			delay := delayCalculator.Next(count)
			if delay == 0 {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				count++
				span.SetAttributes(attribute.Int("retry", count))
				continue
			}
		}
		return output, err
	}
}
