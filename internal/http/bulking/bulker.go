package bulking

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

type Bulker struct {
	ledger   service.Controller
	ledgerID uint32
	tracer   trace.Tracer
}

func (b *Bulker) run(ctx context.Context, bulk Bulk, result chan *LedgerActionResult, continueOnFailure bool) bool {
	hasError := false

	index := 0
	for element := range bulk {
		ctx, span := b.tracer.Start(ctx, "Bulk:ProcessElement",
			trace.WithNewRoot(),
			trace.WithLinks(trace.LinkFromContext(ctx)),
			trace.WithAttributes(attribute.Int("index", index)),
		)

		select {
		case <-ctx.Done():
			result <- NewLedgerActionResult(index, nil, ctx.Err())
			span.End()
			index++
			continue
		default:
		}

		if hasError && !continueOnFailure {
			result <- NewLedgerActionResult(index, nil, context.Canceled)
			span.End()
			index++
			continue
		}

		log, err := b.processElement(ctx, element)
		if err != nil {
			hasError = true
			otlp.RecordError(ctx, err)
			result <- NewLedgerActionResult(index, nil, err)
			span.End()
			index++
			continue
		}

		result <- NewLedgerActionResult(index, log, nil)
		span.End()
		index++
	}

	close(result)

	return hasError
}

func (b *Bulker) Run(ctx context.Context, bulk Bulk, result chan *LedgerActionResult, bulkOptions BulkingOptions) error {
	ctx, span := b.tracer.Start(ctx, "Bulk:Run", trace.WithAttributes(
		attribute.Bool("atomic", bulkOptions.Atomic),
		attribute.Bool("continueOnFailure", bulkOptions.ContinueOnFailure),
	))
	defer span.End()

	// Note: Atomic transactions are not yet supported in this implementation
	if bulkOptions.Atomic {
		return fmt.Errorf("atomic bulk transactions are not yet supported")
	}

	hasError := b.run(ctx, bulk, result, bulkOptions.ContinueOnFailure)
	if hasError && bulkOptions.Atomic {
		// Would rollback here if atomic transactions were supported
		return nil
	}

	return nil
}

func (b *Bulker) processElement(ctx context.Context, elem *servicepb.LedgerApplyAction) (*commonpb.LedgerLog, error) {
	// Set the ledger ID on the action before applying
	elem.LedgerId = b.ledgerID
	logs, err := b.ledger.Apply(ctx, &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: elem,
		},
	})
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("no logs returned")
	}
	// Extract the LedgerLog from the ApplyLog payload
	return logs[0].GetApply().GetLog(), nil
}

func NewBulker(ledgerCluster service.Controller, ledgerID uint32, options ...BulkerOption) *Bulker {
	ret := &Bulker{
		ledger:   ledgerCluster,
		ledgerID: ledgerID,
	}
	for _, option := range append(defaultBulkerOptions, options...) {
		option(ret)
	}

	return ret
}

type BulkerOption func(bulker *Bulker)

func WithTracer(tracer trace.Tracer) BulkerOption {
	return func(options *Bulker) {
		options.tracer = tracer
	}
}

var defaultBulkerOptions = []BulkerOption{
	WithTracer(noop.Tracer{}),
}

type BulkingOptions struct {
	ContinueOnFailure bool
	Atomic            bool
}

type BulkerFactory interface {
	CreateBulker(ctrl service.Controller, ledgerID uint32) *Bulker
}

type DefaultBulkerFactory struct {
	Options []BulkerOption
}

func (d *DefaultBulkerFactory) CreateBulker(ledgerCluster service.Controller, ledgerID uint32) *Bulker {
	return NewBulker(ledgerCluster, ledgerID, d.Options...)
}

func NewDefaultBulkerFactory(options ...BulkerOption) *DefaultBulkerFactory {
	return &DefaultBulkerFactory{
		Options: options,
	}
}

var _ BulkerFactory = (*DefaultBulkerFactory)(nil)
